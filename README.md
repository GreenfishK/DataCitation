# Introduction
In order for the functions of this framework to work a triple store supporting SRAPQL* and RDF* needs to be setup, either locally or remotely. The only two parameters that need to be configured are the GET/read and POST/write endpoints. This is done by instantiating the class SPARQLAPI and passing the two endpoint URIs to the constructor. These URIs, however, vary from solution to solution.

Examples for a locally set up "GraphDB free" database are found in the file "Playground". 


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

    sqlite3 query_store.db "Create table query (
    query_checksum CHAR (200) PRIMARY KEY,
    orig_query CHAR (4000),
    normal_query CHAR (4000)
    )"

    sqlite3 query_store.db "Create table query_citation (
    query_PID CHAR (500) PRIMARY KEY,
    query_checksum CHAR (200),
    result_set_checksum CHAR (200),
    result_set_desc CHAR (4000),
    citation_timestamp DATETIME,
    citation_snippet CHAR (4000),
    foreign key(query_checksum) references query(query_checksum)
    )"


citation_data
/*
might be a dict in JSON or XML format
and contain different citation data sets (e.g. dublin core, dataverse, FORCE11...)
*/

Test database by selecting from it. This can be done by issuing following statement via CLI:

    sqlite3
    .open query_store.db
    select * from query_store.query;

An empty result set should be returned.

Test database by inserting a record. This can be done by typing following into the command line:

    insert into citation_data values (1,"ABC", "checksumABC", "checksumResultABC", "Select * from dual", "Select * from dual", "2021-02-14 15:12:19", "2021-02-14 15:12:19", "some description", "some JSON object");

The inserted row should be returned. Now let's remove the row again.

    Delete from citation_data;

Quit the sqlite3 editor with:

    .quit
