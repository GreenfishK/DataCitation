import SPARQLAPI
import QueryUtils


def cite(self, select_statement, prefixes, result_set_description):
    # citation_text = ""
    # query = Query(select_statement, prefixes)
    # normalized_query_algebra = query.normalize_query_tree()
    # query.compute_checksum("query", normalized_query_algebra)
    #
    # citation_timestamp = datetime.now(timezone(timedelta(hours=2)))
    # query = query.extend_query_with_timestamp(citation_timestamp)
    # query = query.extend_query_with_sort_operation()
    # query_result = SPARQLAPI.get_data(query.query, query.sparql_prefixes)
    # query.compute_checksum("result", query_result)
    #
    # existing_query = query_store.lookup(checksum_query) # --> Query
    # if existing_query:
    #     if query.result_checksum == existing_query.result_checksum:
    #         return existing_query.query_pid
    #     else
    #         query.generate_query_pid()
    #         query_store.append_citation_date(query, citation_timestamp)
    #         return query.query_pid
    # execute query

    # compute hash value of result set

    # compare hash values

    # generate citation text

    # case 2: query does not exist:

    # generate query PID
    query_pid = query.generate_query_pid()

    # execute query

    # compute hash value of result set

    # store: query PID, query checksum, query, normalized query, version timestamp, execution timestamp, result
    # result set check sum, result set description

    return citation_text

    # embed query timestamp (max valid_from of dataset)
