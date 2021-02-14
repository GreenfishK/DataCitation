import DataCitationFramework.SPARQLAPI as sa
import DataCitationFramework.QueryUtils as qu
import datetime
import DataCitationFramework.QueryStore as qs
from datetime import datetime, timedelta, timezone


def generate_citation_snippet(query_pid: str, result_set_desc: str, citation_data: dict) -> str:
    snippet = "query_pid: {0} \n".format(query_pid)
    snippet += "Result set description: {0} \n".format(result_set_desc)
    for key, value in citation_data.items():
        snippet += "{0}: {1} \n".format(key, value)
    return snippet


def cite(select_statement, prefixes, result_set_desc: str, citation_data: dict):
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

    :param result_set_desc:
    :param citation_data:
    :param select_statement:
    :param prefixes:
    :return:
    """

    query = qu.Query(select_statement, prefixes)
    normalized_query_algebra = query.normalize_query_tree()
    query.compute_checksum("query", normalized_query_algebra)
    query.citation_timestamp = datetime.now(timezone(timedelta(hours=2)))
    query.generate_query_pid(query.citation_timestamp, query.query_checksum)
    query_pid = query.query_pid
    timestamped_query = query.extend_query_with_timestamp(query.citation_timestamp)
    sorted_query = query.extend_query_with_sort_operation(timestamped_query)

    sparqlapi = sa.SPARQLAPI('http://192.168.0.241:7200/repositories/DataCitation',
                             'http://192.168.0.241:7200/repositories/DataCitation/statements')
    query_result = sparqlapi.get_data(sorted_query, prefixes)
    query.compute_checksum("result", query_result)

    query_store = qs.QueryStore("query_store.db")
    existing_query = query_store.lookup(query.query_checksum)  # --> Query
    if existing_query:
        if query.result_set_checksum == existing_query.result_set_checksum:
            query_pid = existing_query.query_pid
        else:
            query_store.insert_new_query_version(query, query.citation_timestamp)
            query.generate_query_pid(query.citation_timestamp, query.query_checksum)
            query_pid = query.query_pid
        citation_snippet = query_store.citation_snippet(query_pid)
        return citation_snippet
    else:
        citation_snippet = generate_citation_snippet(query_pid, result_set_desc, citation_data)
        query_store.store(query, citation_snippet)
        return citation_snippet

    # embed query timestamp (max valid_from of dataset)
