update query_hub
set last_execution_pid = (select b.query_pid
                        from query_hub a
                        join query_satellite b
                        on a.query_checksum = b.query_checksum
                        where a.query_checksum = :query_checksum
                        and execution_timestamp = (select max(execution_timestamp) from query_satellite c
                                                  where a.query_checksum = c.query_checksum))