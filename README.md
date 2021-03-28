# Introduction
In order for the functions of this framework to work a triple store supporting SRAPQL* and RDF* and a query store need to be setup. The only two parameters that need to be configured are the GET/read and POST/write endpoints. This is done by instantiating the class SPARQLAPI and passing the two endpoint URIs to the constructor. These URIs, however, vary from solution to solution. Examples for a locally set up "GraphDB free" database are found in the file "Playground". 
The query store is based on the citation data attributes from the literature (Dublin core, dataverse, FORCE11...) but it rather uses a common set of attributes and does not fully replicate any of these sets.

# Libraries needed:
* rdflib: conda install -c conda-forge rdflib
* bsddb3: conda install -c conda-forge bsddb3
* prettytable: conda install -c conda-forge prettytable
* SPARQLWrapper: conda install -c conda-forge sparqlwrapper
* sqlite3
* json
* sqlalchemy
* nested-lookup

The libraries were installed in a virtual conda environment labeled 'TripleStoreCitationFramework'

# Database setup
Create db and necessary tables by typing following code into the command line:

    sqlite3 query_store.db 

    Create table query_hub (
    query_checksum CHAR (200) PRIMARY KEY,
    orig_query CHAR (4000),
    normal_query CHAR (4000)
    );

    Create table query_citation (
    query_pid CHAR (500) PRIMARY KEY,
    query_checksum CHAR (200),
    result_set_checksum CHAR (200),
    citation_timestamp DATETIME,
    citation_data CHAR (4000),
    citation_snippet CHAR (4000),
    foreign key(query_checksum) references query_hub(query_checksum)
    );


Test database by inserting data and executing a query with both tables joined. This can be done by issuing following statement via CLI:

    sqlite3
    .open query_store.db
    insert into query_hub values (123, 'Select * from dual', 'Select a, b, c from dual');
    insert into query_citation values (1, 123, 333443, 'some description', '2020-09-08T12:11:21.941+02:00', 'Some citation snippet');
    insert into query_citation values (2, 123, 123132, 'some other description', '2021-01-08T12:11:21.941+02:00', 'Some other citation snippet');

    sqlite3
    .open query_store.db
    select b.query_pid, a.query_checksum, a.orig_query, a.normal_query,
    b.result_set_checksum, b.result_set_desc, b.citation_timestamp, b.citation_snippet
    from query a join query_citation b
    on a.query_checksum = b.query_checksum
    where a.query_checksum = 123 and citation_timestamp =
    (select max(citation_timestamp) from query_citation c
    where a.query_checksum = c.query_checksum);

One record should be returned where the record from the query_citation table will be the one with the most recent citation date.
Quit the sqlite3 editor with:

    .quit
