insert into query_satellite(query_pid, query_checksum, timestamped_query, result_set_checksum,
                           result_set_description, result_set_sort_order, citation_data,
                           citation_snippet, execution_timestamp)
values (:query_pid, :query_checksum, :timestamped_query, :result_set_checksum, :result_set_description,
:result_set_sort_order, :citation_data, :citation_snippet, :execution_timestamp)