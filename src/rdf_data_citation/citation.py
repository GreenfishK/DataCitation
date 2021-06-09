import pandas as pd

from query_store import QueryStore
from rdf_star import TripleStoreEngine
from citation_utils import RDFDataSetUtils, QueryUtils, MetaData, generate_citation_snippet
from _helper import citation_timestamp_format
from _exceptions import MissingSortVariables, SortVariablesNotInSelectError, \
    ExpressionNotCoveredException, NoUniqueSortIndexError, QueryDoesNotExistError
import logging
from copy import copy
import datetime
from datetime import datetime, timedelta, timezone
import tzlocal
import json


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

        query_store = QueryStore()

        # Assign citation timestamp to query object
        current_datetime = datetime.now()
        timezone_delta = tzlocal.get_localzone().dst(current_datetime).seconds
        execution_datetime = datetime.now(timezone(timedelta(seconds=timezone_delta)))
        execution_timestamp = citation_timestamp_format(execution_datetime)
        self.execution_timestamp = execution_timestamp

        try:
            query_to_cite = QueryUtils(select_statement, citation_timestamp)
        except ExpressionNotCoveredException as e:
            raise ExpressionNotCoveredException(e)

        # Execute query
        result_set = self.sparqlapi.get_data(select_statement, citation_timestamp)

        # Validate order by clause
        if len(query_to_cite.order_by_variables) > 1:
            logging.warning("Multiple order by clauses were found. The first one found will be considered. "
                            "In most cases this should be the outer most clause.")
        try:
            order_by_variables = [v.n3()[1:] for v in query_to_cite.order_by_variables[0]]
        except KeyError:
            raise MissingSortVariables("There is no order by clause. Please provide an order by clause with "
                                       "variables that yield a unique sort.")
        try:
            yn_unique_sort_index = result_set.set_index(order_by_variables).index.is_unique
        except KeyError:
            raise SortVariablesNotInSelectError("There are variables in the order by clause that are not listed "
                                                "in the select clause. While this is syntactically correct "
                                                "a unique sort index should only contain variables from the "
                                                "select clause or dataset columns respectively.")

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

        # Get latest query citation and its metadata by query checksum
        existing_query_data, existing_query_rdf_ds_data, existing_query_citation_data \
            = query_store.get_latest_citation(query_to_cite.checksum)

        if existing_query_data and existing_query_rdf_ds_data and existing_query_citation_data:
            logging.info("Query was found in query store.")
            self.yn_query_exists = True
            if rdf_ds.checksum == existing_query_rdf_ds_data.checksum:
                self.query_utils = existing_query_data
                self.result_set_utils = existing_query_rdf_ds_data
                self.citation_metadata = existing_query_citation_data
                logging.info("The result set has not changed since the last execution. "
                             "The existing citation snippet will be returned.")
                return
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
                logging.info("The dataset changed since the last citation. The citation and dataset metadata of query "
                             "with PID {0} were updated with the new PID {1}".format(existing_query_data.pid,
                                                                                     query_to_cite.pid))
                query_store.store(query_to_cite, rdf_ds, self.citation_metadata, yn_new_query=False)
        else:
            logging.info("A new query citation with PID {0} was stored in the query store.".format(query_to_cite.pid))
            query_store.store(query_to_cite, rdf_ds, self.citation_metadata, yn_new_query=True)

    def retrieve(self, query_pid: str) -> [pd.DataFrame, str]:
        """
        Retrieves query data, the dataset and its metadata, citation metadata and the citation snippet by the query_pid
        and returns it as a JSON file to foster machine actionability.
        This is done by  first querying aforementioned data from the query store followed by an execution of
        the timestamped query against the RDF store, thus, post_endpoint (post can carry more data then get
        and the timestamped queries tend to be quiet long).
        Use a landing page to display these data. (R11 and R12)

        R11 – Landing Page: Make the PIDs resolve to a human readable landing page that provides the data
        (via query re-execution) and metadata, including a link to the superset (PID of the data source)
        and citation text snippet.[1]
        R12  – Machine Actionability:  Provide  an  API  / machine actionable
        landing page to access metadata and data via query re-execution.[1]

        [1]: Data Citation of Evolving Data: Andreas Rauber, Ari Asmi, Dieter van Uytvanck and Stefan Pröll

        :param query_pid:
        :return: The cited (historic) dataset.
        """

        query_store = QueryStore()
        try:
            query_data, result_set_data, citation_metadata = query_store.get_cited_query(query_pid)
        except QueryDoesNotExistError as e:
            raise QueryDoesNotExistError("{0} The query and its metadata will not be retrieved.".format(e))
        result_set_data.dataset = self.sparqlapi.get_data(query_data.timestamped_query, yn_timestamp_query=False)
        metadata = json.dumps({'query_data': {'query': query_data.query,
                                              'timestamped_query': query_data.timestamped_query,
                                              'prefixes': query_data.sparql_prefixes,
                                              'normal_query_algebra': query_data.normal_query_algebra,
                                              'normal_query': query_data.normal_query,
                                              'citation_timestamp': query_data.citation_timestamp,
                                              'pid': query_data.pid,
                                              'checksum': query_data.checksum},
                               'dataset_metadata': {'description': result_set_data.description,
                                                    'sort_order': result_set_data.sort_order,
                                                    'checksum': result_set_data.checksum},
                               'citation_metadata': {'identifier': citation_metadata.identifier,
                                                     'creator': citation_metadata.creator,
                                                     'title': citation_metadata.title,
                                                     'publisher': citation_metadata.publisher,
                                                     'publication_year': citation_metadata.publication_year,
                                                     'resource_type': citation_metadata.resource_type,
                                                     'other_citation_data': citation_metadata.other_citation_data},
                               'citation_snippet': citation_metadata.citation_snippet}, indent=4)

        return [result_set_data.dataset, metadata]



