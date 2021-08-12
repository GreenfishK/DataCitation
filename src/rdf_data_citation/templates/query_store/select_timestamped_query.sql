select a.query_checksum, a.orig_query, b.timestamped_query, a.query_prefixes, a.normal_query, a.normal_query_algebra, a.last_execution_pid,
b.query_pid, b.result_set_description, b.result_set_sort_order, b.execution_timestamp,
b.result_set_checksum, b.citation_data, b.citation_snippet
from query_hub a
join query_satellite b
on (a.query_checksum = b.query_checksum)
where b.query_pid = :query_pid