from src.rdf_data_citation.query_store import QueryStore
from src.rdf_data_citation.rdf_star import TripleStoreEngine
from src.rdf_data_citation.citation_utils import CitationData, RDFDataSetData, QueryData, _intersection, generate_citation_snippet
from src.rdf_data_citation.citation_utils import NoUniqueSortIndexError
import datetime
from datetime import datetime, timedelta, timezone
import tzlocal
from rdflib.term import Variable


class SortVariablesNotInSelectError(Exception):
    pass


class Citation:

    def __init__(self, get_endpoint: str, post_endpoint: str):
        """
        Initializes the Citation class.

        :param get_endpoint: RDF* store URL for get/read statements.
        :param post_endpoint:  RDF* store URL for post/write statements.
        """
        self.sparqlapi = TripleStoreEngine(get_endpoint, post_endpoint)

        self.yn_query_exists = False
        self.yn_result_set_changed = False
        self.yn_unique_sort_index = False
        self.execution_timestamp = None
        self.query_data = QueryData()
        self.result_set_data = RDFDataSetData()
        self.citation_metadata = CitationData()

    def cite(self, select_statement: str, citation_metadata: CitationData, result_set_description: str,
             unique_sort_index: tuple):
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

        :param unique_sort_index:
        :param result_set_description:
        :param citation_metadata:
        :param select_statement:
        :return:
        """

        sparqlapi = self.sparqlapi
        query_store = QueryStore()
        query_to_cite = QueryData(select_statement)

        # Assign citation timestamp to query object
        current_datetime = datetime.now()
        timezone_delta = tzlocal.get_localzone().dst(current_datetime).seconds
        execution_datetime = datetime.now(timezone(timedelta(seconds=timezone_delta)))
        execution_timestamp = execution_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" \
                             + execution_datetime.strftime("%z")[3:5]
        query_to_cite.citation_timestamp = execution_timestamp
        self.execution_timestamp = execution_timestamp

        # Compute query checksum
        query_to_cite.compute_checksum(query_to_cite.normalized_query_algebra)

        # Generate query PID
        query_to_cite.pid = query_to_cite.generate_query_pid()

        # Create query tree and normalize query tree
        # TODO: Check if this is needed because it is already computed durinig init of QueryData
        query_to_cite.normalize_query_tree()

        # Extend query with timestamp
        timestamped_query = query_to_cite.timestamp_query()

        # Execute query
        result_set = sparqlapi.get_data(timestamped_query)

        # Validate order by clause
        order_by_variables = [v.n3()[1:] for v in query_to_cite.order_by_variables]
        try:
            yn_unique_sort_index = result_set.set_index(order_by_variables).index.is_unique
        except KeyError as e:
            raise SortVariablesNotInSelectError("There are variables in the order by clause that are not listed "
                                                "in the select clause. While this is syntactically correct "
                                                "a unique sort index should only contain variables from the "
                                                "select clause, thus, from the dataset.")

        if not yn_unique_sort_index:
            raise NoUniqueSortIndexError('The "order by"-clause in your query does not yield a uniquely sorted '
                                         'dataset. Please provide a primary key or another unique sort index')
        else:
            self.yn_unique_sort_index = True

        # Sort result set
        rdf_ds = RDFDataSetData(dataset=result_set, description=result_set_description)
        # # sort() will create an unique sort index if no unique user sort index is provided.
        rdf_ds.dataset = rdf_ds.sort(tuple(order_by_variables))

        # Compute result set checksum
        rdf_ds.checksum = rdf_ds.compute_checksum()

        # Lookup query by checksum
        # TODO: empty dataset should also be valid
        existing_query_data, existing_query_rdf_ds_data, existing_query_citation_data \
            = query_store.lookup(query_to_cite.checksum)

        if existing_query_data and existing_query_rdf_ds_data and existing_query_citation_data:
            self.yn_query_exists = True
            if rdf_ds.checksum == existing_query_rdf_ds_data.checksum:
                self.query_data = existing_query_data
                self.result_set_data = existing_query_rdf_ds_data
                self.citation_metadata = existing_query_citation_data
                print("Query already exists and the result set has not changed since the last execution. "
                      "The existing citation snippet will be returned.")
            else:
                self.yn_result_set_changed = True
                self.query_data = query_to_cite
                self.result_set_data = rdf_ds
                self.citation_metadata = citation_metadata
                query_store.store(query_to_cite, rdf_ds, citation_metadata, yn_new_query=False)
            return self
        else:
            # Generate citation snippet
            citation_snippet = generate_citation_snippet(query_to_cite.pid, citation_metadata)
            citation_metadata.citation_snippet = citation_snippet

            # Store query object
            self.query_data = query_to_cite
            self.result_set_data = rdf_ds
            self.citation_metadata = citation_metadata
            query_store.store(query_to_cite, rdf_ds, citation_metadata)

            return self

        # TODO: embed query timestamp (max valid_from of dataset). No idea what it means
