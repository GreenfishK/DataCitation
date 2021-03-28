import DataCitationFramework.SPARQLAPI as sa
import DataCitationFramework.QueryUtils as qu
import datetime
import DataCitationFramework.QueryStore as qs
from datetime import datetime, timedelta, timezone
import json


class CitationData:

    def __init__(self, identifier, creator, title, publisher, publication_year, resource_type,
                 other_citation_data: dict = None):
        """
        Initialize the mandatory fields from DataCite's metadata model version 4.3
        """
        self.identifier = identifier
        self.creator = creator
        self.title = title
        self.publisher = publisher
        self.publication_year = publication_year
        self.resource_type = resource_type
        self.other_citation_data = other_citation_data

    def to_json(self):
        citation_data = vars(self)
        citation_data_json = json.dumps(citation_data, indent=4)
        print(citation_data_json)
        return citation_data_json


def read_json(json_string) -> CitationData:
    citation_data_dict = json.loads(json_string)

    citation_data = CitationData(citation_data_dict['identifier'], citation_data_dict['creator'],
                                 citation_data_dict['title'], citation_data_dict['publisher'],
                                 citation_data_dict['publication_year'], citation_data_dict['resource_type'],
                                 citation_data_dict['other_citation_data'])

    return citation_data


def generate_citation_snippet(query_pid: str, citation_data: CitationData) -> str:
    """
    Generates the citation snippet out of DataCite's mandatory attributes within its metadata schema.
    :param query_pid:
    :param citation_data:
    :return:
    """
    # TODO: Which metadata set to use?
    snippet = "{0}, {1}, {2}, {3}, {4}, {5}, query_pid: {6} \n".format(citation_data.identifier,
                                                                       citation_data.creator,
                                                                       citation_data.title,
                                                                       citation_data.publisher,
                                                                       citation_data.publication_year,
                                                                       citation_data.resource_type,
                                                                       query_pid)
    return snippet


def cite(select_statement, prefixes, citation_data: CitationData):
    """
    Persistently Identify Specific Data Sets

    R4: Re-write the query to a normalised form so that identical queries
    can be detected. Compute a checksum of the normalized query to efficiently detect identical queries.

    R5: Ensure that the sorting of the records in the data set is unambiguous and reproducible

    R6:Compute  fixity information (checksum) of the query result set to enable verification
    of the correctness of a result upon re-execution

    R7: Assign a timestamp to the query based on the last update to the entire database
    (or the last update to the selection of data affected by the query or the query execution time).
    This allows retrieving the data as it existed at the time a user issued a query.

    The timestamp of the first citation since the last update to the selection of the data
    affected by the query will be taken.

    R8: Assign a new PID to the query if either the query is new or if the result set returned from an earlier
    identical query is different due to changes in the data. Otherwise, return the existing PID.

    R9: Store query and metadata (e.g. PID, original and normalized  query, query & result set checksum,
    timestamp, superset  PID, data  set description, and other) in the query store.

    R10: Generate citation texts  in  the  format  prevalent  in  the  designated community for lowering the barrier
    for citing the data. Include the PID into the citation text snippet.

    :param citation_data:
    :param select_statement:
    :param prefixes:
    :return:
    """
    sparqlapi = sa.SPARQLAPI('http://192.168.0.241:7200/repositories/DataCitation',
                             'http://192.168.0.241:7200/repositories/DataCitation/statements')
    query_store = qs.QueryStore("query_store.db")
    query_to_cite = qu.Query(select_statement, prefixes)

    # Assign citation timestamp to query object
    citation_datetime = datetime.now(timezone(timedelta(hours=2)))
    citation_timestamp = citation_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" \
                         + citation_datetime.strftime("%z")[3:5]
    query_to_cite.citation_timestamp = citation_timestamp

    # Create query tree and normalize query tree
    # TODO: create query tree outside of normalize query tree
    query_to_cite.normalize_query_tree()
    # Compute query checksum
    query_to_cite.compute_checksum("query", query_to_cite.normalized_query_algebra)
    # Lookup query by checksum
    existing_query = query_store.lookup(query_to_cite.query_checksum)  # --> Query

    # Extend query with timestamp
    timestamped_query = query_to_cite.extend_query_with_timestamp()
    # Extend query with sort operation
    sorted_query = query_to_cite.extend_query_with_sort_operation(timestamped_query)
    # Execute query and retrieve result set
    query_result = sparqlapi.get_data(sorted_query, prefixes)
    # Compute result set checksum
    query_to_cite.compute_checksum("result", query_result)

    if existing_query:
        if query_to_cite.result_set_checksum == existing_query.result_set_checksum:
            # Retrieve citation snippet from query store
            citation_snippet = query_store.citation_snippet(existing_query.query_pid)
            return citation_snippet
    query_to_cite.generate_query_pid(query_to_cite.citation_timestamp, query_to_cite.query_checksum)
    # Generate citation snippet
    citation_snippet = generate_citation_snippet(query_to_cite.query_pid, citation_data)
    # Store query object
    query_store.store(query_to_cite, citation_data.to_json(), citation_snippet)
    return citation_snippet

    # embed query timestamp (max valid_from of dataset)


