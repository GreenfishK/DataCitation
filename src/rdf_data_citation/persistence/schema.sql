Create table query_hub (
query_checksum CHAR (200) PRIMARY KEY,
orig_query CHAR (4000),
query_prefixes CHAR (4000),
last_citation_pid CHAR (4000),
normal_query CHAR (4000)
);

Create table query_citation (
query_pid CHAR (500) PRIMARY KEY,
query_checksum CHAR (200),
result_set_checksum CHAR (200),
result_set_description CHAR (4000),
result_set_sort_order CHAR (4000),
citation_timestamp DATETIME,
citation_data CHAR (4000),
citation_snippet CHAR (4000),
foreign key(query_checksum) references query_hub(query_checksum)
);