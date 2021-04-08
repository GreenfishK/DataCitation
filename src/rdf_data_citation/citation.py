import datetime
from datetime import datetime, timedelta, timezone
from citation_utils import CitationData, RDFDataSetData, QueryData, _intersection, generate_citation_snippet
from query_store import QueryStore
from rdf_star import TripleStoreEngine
from rdflib.term import Variable


class Citation:

    def __init__(self, get_endpoint: str, post_endpoint: str):
        """
        Initializes the Citation class.

        :param get_endpoint: RDF* store URL for get/read statements.
        :param post_endpoint:  RDF* store URL for post/write statements.
        """
        self.sparqlapi = TripleStoreEngine(get_endpoint, post_endpoint)

    def cite(self, select_statement: str, prefixes: dict, citation_data: CitationData, result_set_description: str):
        """
        Persistently Identify Specific Data Sets

        R4: Re-write the query to a normalised form so that identical queries
        can be detected. Compute a checksum of the normalized query to efficiently detect identical queries. [1]

        R5: Ensure that the sorting of the records in the data set is unambiguous and reproducible [1]

        R6:Compute  fixity information (checksum) of the query result set to enable verification
        of the correctness of a result upon re-execution [1]

        R7: Assign a timestamp to the query based on the last update to the entire database
        (or the last update to the selection of data affected by the query or the query execution time).
        This allows retrieving the data as it existed at the time a user issued a query. [1]

        The timestamp of the first citation since the last update to the selection of the data
        affected by the query will be taken.

        R8: Assign a new PID to the query if either the query is new or if the result set returned from an earlier
        identical query is different due to changes in the data. Otherwise, return the existing PID. [1]

        R9: Store query and metadata (e.g. PID, original and normalized  query, query & result set checksum,
        timestamp, superset  PID, data  set description, and other) in the query store. [1]

        R10: Generate citation texts  in  the  format  prevalent  in  the  designated community for lowering the barrier
        for citing the data. Include the PID into the citation text snippet. [1]
        Provenance information should be included in a citation snippet in order to cite the correct version,
        manipulation, and transformation of data [2]

        [1]: Data Citation of Evolving Data: Andreas Rauber, Ari Asmi, Dieter van Uytvanck and Stefan Pröll
        [2]: Theory and Practice of Data Citation, Gianmaria Silvello

        :param result_set_description:
        :param citation_data:
        :param select_statement:
        :param prefixes:
        :return:
        """

        sparqlapi = self.sparqlapi
        query_store = QueryStore("persistence/query_store.db")
        query_to_cite = QueryData(select_statement, prefixes)

        # Assign citation timestamp to query object
        citation_datetime = datetime.now(timezone(timedelta(hours=2)))
        citation_timestamp = citation_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" \
                             + citation_datetime.strftime("%z")[3:5]
        query_to_cite.citation_timestamp = citation_timestamp

        # Create query tree and normalize query tree
        # TODO: create query tree outside of normalize query tree
        query_to_cite.normalize_query_tree()
        # Compute query checksum
        query_to_cite.compute_checksum(query_to_cite.normalized_query_algebra)
        # Lookup query by checksum
        existing_query = query_store.lookup(query_to_cite.checksum)  # --> QueryData

        # Extend query with timestamp
        timestamped_query = query_to_cite.decorate_query()

        # Extend query with sort operation. Use the index suggestor to suggest the index to use for sorting
        query_result = sparqlapi.get_data(timestamped_query, prefixes)
        query_tree_variables = []
        for v in query_to_cite.variables:
            if isinstance(v, Variable):
                query_tree_variables.append(v.n3()[1:])

        dataset_variables = _intersection(query_result.columns, query_tree_variables)
        rdf_ds = RDFDataSetData(query_result[dataset_variables])
        rdf_ds.description = result_set_description
        sorted_ds = rdf_ds.sort()
        rdf_ds.dataset = sorted_ds
        rdf_ds.checksum = rdf_ds.compute_checksum()

        # Check whether query already exists
        existing_query_data, existing_query_rdf_ds_data, existing_query_citation_data \
            = query_store.lookup(query_to_cite.checksum)

        if existing_query:
            if rdf_ds.checksum == existing_query_rdf_ds_data.checksum:
                citation_snippet = existing_query_citation_data.citation_snippet
                return citation_snippet
            else:
                pass
        # Generate citation snippet
        citation_snippet = generate_citation_snippet(query_to_cite.pid, citation_data)
        citation_data.citation_snippet = citation_snippet
        # Store query object
        query_store.store(query_to_cite, rdf_ds, citation_data)
        return citation_snippet

        # embed query timestamp (max valid_from of dataset)
