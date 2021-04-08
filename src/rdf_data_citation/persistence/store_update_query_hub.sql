update query_hub
set last_citation_pid = (select b.query_pid
                        from query_hub a
                        join query_citation b
                        on a.query_checksum = b.query_checksum
                        where a.query_checksum = :query_checksum
                        and citation_timestamp = (select max(citation_timestamp) from query_citation c
                                                  where a.query_checksum = c.query_checksum))