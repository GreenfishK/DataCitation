select a.query_checksum, a.orig_query, a.query_prefixes, a.normal_query, a.last_citation_pid,
b.query_pid, b.result_set_description, b.result_set_sort_order, b.citation_timestamp,
b.result_set_checksum, b.citation_data, b.citation_snippet
from query_hub a
join query_citation b
on (a.query_checksum = b.query_checksum and a.last_citation_pid = b.query_pid)
where a.query_checksum = :query_checksum