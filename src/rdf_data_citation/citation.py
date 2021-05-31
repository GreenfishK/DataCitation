import logging

from src.rdf_data_citation.query_store import QueryStore
from src.rdf_data_citation.rdf_star import TripleStoreEngine
from src.rdf_data_citation.citation_utils import RDFDataSetUtils, QueryUtils, MetaData, generate_citation_snippet
from src.rdf_data_citation.citation_utils import NoUniqueSortIndexError
from src.rdf_data_citation._helper import citation_timestamp_format
from src.rdf_data_citation.exceptions import MissingSortVariables, SortVariablesNotInSelectError
from copy import copy
import datetime
from datetime import datetime, timedelta, timezone
import tzlocal


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
        self.query_utils = QueryUtils()
        self.result_set_utils = RDFDataSetUtils()
        self.citation_metadata = MetaData()

    def cite(self, select_statement: str, citation_metadata: MetaData, result_set_description: str = None,
             citation_timestamp: datetime = None):
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

        :param citation_timestamp: If this parameter is left out the current system datetime will be used as citation
        timestamp.
        :param result_set_description:
        :param citation_metadata:
        :param select_statement:
        :return:
        """

        print("Citing... ")
        query_store = QueryStore()

        # Assign citation timestamp to query object
        current_datetime = datetime.now()
        timezone_delta = tzlocal.get_localzone().dst(current_datetime).seconds
        execution_datetime = datetime.now(timezone(timedelta(seconds=timezone_delta)))
        execution_timestamp = citation_timestamp_format(execution_datetime)
        self.execution_timestamp = execution_timestamp

        if not citation_timestamp:
            query_to_cite = QueryUtils(select_statement)
        else:
            query_to_cite = QueryUtils(select_statement, citation_timestamp)

        # Execute query
        result_set = self.sparqlapi.get_data(select_statement, citation_timestamp)

        # Validate order by clause
        if len(query_to_cite.order_by_variables) > 1:
            print("Multiple order by clauses were found. The first one found will be considered. In most cases this"
                  "should be the outer most clause.")
        order_by_variables = [v.n3()[1:] for v in query_to_cite.order_by_variables[0]]
        if not order_by_variables:
            raise MissingSortVariables("There is no order by clause. Please provide an order by clause with "
                                       "variables that yield a unique sort.")
        try:
            yn_unique_sort_index = result_set.set_index(order_by_variables).index.is_unique
        except KeyError:
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
        rdf_ds = RDFDataSetUtils(dataset=result_set)
        # # sort() will create an unique sort index if no unique user sort index is provided.
        rdf_ds.dataset = rdf_ds.sort(tuple(order_by_variables))
        rdf_ds.description = rdf_ds.describe(result_set_description)

        # Compute result set checksum
        rdf_ds.checksum = rdf_ds.compute_checksum()

        # Lookup query by checksum
        existing_query_data, existing_query_rdf_ds_data, existing_query_citation_data \
            = query_store.lookup(query_to_cite.checksum)

        if existing_query_data and existing_query_rdf_ds_data and existing_query_citation_data:
            logging.info("Query was found in query store.")
            self.yn_query_exists = True
            if rdf_ds.checksum == existing_query_rdf_ds_data.checksum:
                self.query_utils = existing_query_data
                self.result_set_utils = existing_query_rdf_ds_data
                self.citation_metadata = existing_query_citation_data
                logging.info("The result set has not changed since the last execution. "
                             "The existing citation snippet will be returned.")
                return self
            else:
                self.yn_result_set_changed = True

        # Store new query data
        self.query_utils = query_to_cite
        self.result_set_utils = rdf_ds
        # TODO: assign identifier in citation_metadata.identifier
        citation_snippet = generate_citation_snippet(query_to_cite.pid, citation_metadata)
        self.citation_metadata = copy(citation_metadata)
        self.citation_metadata.citation_snippet = citation_snippet

        if self.yn_query_exists:
            if self.yn_result_set_changed:
                query_store.store(query_to_cite, rdf_ds, self.citation_metadata, yn_new_query=False)
        else:
            query_store.store(query_to_cite, rdf_ds, self.citation_metadata, yn_new_query=True)

        return self

        # TODO: embed query timestamp (max valid_from of dataset). No idea what it means

