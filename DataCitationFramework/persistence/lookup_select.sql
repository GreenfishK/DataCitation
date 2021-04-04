select b.query_pid, a.query_checksum, b.result_set_checksum, b.citation_snippet
from query_hub a
join query_citation b
on (a.query_checksum = b.query_checksum and a.last_citation_pid = b.query_pid)