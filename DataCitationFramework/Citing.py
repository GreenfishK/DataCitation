import SPARQLAPI as sa
import QueryUtils as qu
import datetime
import QueryStore as query_store
from datetime import datetime, timedelta, timezone


def generate_citation_snippet(citation_data: dict) -> str:
    return "citation snippet"


def cite(select_statement, prefixes, result_set_description):
    query = qu.Query(select_statement, prefixes)
    normalized_query_algebra = query.normalize_query_tree()

    query.compute_checksum("query", normalized_query_algebra)
    citation_timestamp = datetime.now(timezone(timedelta(hours=2)))
    query.generate_query_pid(query.citation_timestamp, query.query_checksum)
    query_pid = query.query_pid

    timestamped_query = query.extend_query_with_timestamp(citation_timestamp)
    sorted_query = query.extend_query_with_sort_operation(timestamped_query)
    query_result = sa.SPARQLAPI.get_data(sorted_query, prefixes)
    query.compute_checksum("result", query_result)

    existing_query = query_store.lookup(query.query_checksum)  # --> Query
    if existing_query:
        if query.result_checksum == existing_query.result_checksum:
            query_pid = existing_query.query_pid
        else:
            query_store.append_citation_date(query, citation_timestamp)
    else:
        citation_snippet = generate_citation_snippet({'contributor': 'Filip',
                                                  'citation_timestamp': citation_timestamp,
                                                  'query_pid': query_pid})
        query_store.store(query, citation_snippet, result_set_description)
    citation_snippet = query_store.citation_snippet(query_pid)

    return citation_snippet

    # embed query timestamp (max valid_from of dataset)
