Create table query (
query_checksum CHAR (200) PRIMARY KEY,
orig_query CHAR (4000),
normal_query CHAR (4000),
)

Create table query_citation (
query_PID CHAR (500) PRIMARY KEY,
query_checksum CHAR (200),
result_set_checksum CHAR (200),
result_set_desc CHAR (4000),
citation_timestamp DATETIME,
citation_snippet CHAR (4000),
foreign key(query_checksum) references query(query_checksum)
)

/*
Test Data
*/

insert into query values (123, "Select * from dual", "Select a, b, c from dual")
insert into query_citation values (1, 123, 333443, "some description", 2020-09-08T12:11:21.941, "Some citation snippet")
insert into query_citation values (2, 123, 123132, "some other description", 2021-01-08T12:11:21.941, "Some other citation snippet")
