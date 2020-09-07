from rdflib.plugins.sparql import prepareQuery
from rdflib.term import Node

import GraphDB.connectToGraphDB as con
from prettytable import PrettyTable
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON
from rdflib import URIRef, Literal, BNode, Graph
from rdflib.namespace import DC, FOAF


def _init_ns_to_nodes(initNs):
    """
    :param initNs: prefixes to use as a dict where the key is the prefix and the value which the key resolves to.
    :return: the initial Namespace dictionary where the values are node objects
    """

    pfx = {}
    for key in initNs:
        node = URIRef(initNs[key])
        pfx[key] = node
        print("set %s = %s" % (key, node))
    return pfx


def print_triples(qres):
    """
    :param qres: result of the SPARQL select statement in JSON format
    :return:
    """
    t = PrettyTable(['subjectToUpdate', 'predicateToUpdate', 'objectToUpdate'])
    for row in qres["results"]["bindings"]:
        t.add_row([row["subjectToUpdate"]["value"],
                   row["predicateToUpdate"]["value"],
                   row["objectToUpdate"]["value"],
                   ])

    print(t)


class DataVersioning:
    """
    NOTE: Prepared Queries cannot be used with SPARQL* and RDF* as both are not yet supported in rdflib.
    That is why we need to write queries as plain strings.
    """

    def __init__(self, query_endpoint, update_endpoint, prefixes=None, credentials=None):
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint
        self.prefixes = prefixes
        self.sparql_get = SPARQLWrapper(query_endpoint)
        self.sparql_post = SPARQLWrapper(update_endpoint)
        self.credentials = credentials

        self.sparql_post.setHTTPAuth(DIGEST)
        self.sparql_post.setMethod(POST)

        self.sparql_get.setHTTPAuth(DIGEST)
        self.sparql_get.setMethod(GET)
        self.sparql_get.setReturnFormat(JSON)

        if self.credentials is not None:
            self.sparql_post.setCredentials(credentials)
            self.sparql_get.setCredentials(credentials)

    def reset_all_versions(self):
        delete_statement = """
        PREFIX citing: <http://ontology.ontotext.com/citing/>
        # reset versions 
        delete {
            ?s citing:valid_from ?o  ;  
               citing:valid_until ?o   
        }
        where
        {
           ?s ?p ?o .
        }
        """

        self.sparql_post.setQuery(delete_statement)
        self.sparql_post.query()
        print("All annotations have been removed.")

    def version_all_rows(self):
        """
        Version all rows with the current timestamp.
        :return:
        """
        update_statement = """
        PREFIX citing: <http://ontology.ontotext.com/citing/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        insert 
        {
            <<?s ?p ?o>> citing:valid_from ?currentTimestamp;
                         citing:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime.        
        }
        where
        {
           ?s ?p ?o .
           BIND(xsd:dateTime(NOW()) AS ?currentTimestamp).
        }"""

        self.sparql_post.setQuery(update_statement)
        self.sparql_post.query()
        print("All rows have been annotated with the current timestamp")

    def get_triples_to_update(self, statement):
        """
        :param statement: a select statement returning a set of triples where the object should be updated. The
        returned variables must be as follows: ?subjectToUpdate, ?predicateToUpdate, ?objectToUpdate
        :return: a set of triples in JSON format where the object should be updated.
        """

        self.sparql_get.setQuery(statement)
        qres = self.sparql_get.query().convert()
        print_triples(qres)

        return qres

    def update(self, select_statement, new_value):
        statement = """
            PREFIX pub: <http://ontology.ontotext.com/taxonomy/>
            PREFIX citing: <http://ontology.ontotext.com/citing/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            
            delete {
                <<?subjectToUpdate ?predicateToUpdate ?objectToUpdate>> citing:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime
            }
            insert {
                # outdate old triple with date as of now()
                <<?subjectToUpdate ?predicateToUpdate ?objectToUpdate>> citing:valid_until ?newVersion.
                
                # update new row with value and timestamp as of now()
                ?subjectToUpdate ?predicateToUpdate ?newValue . # new value
                # assign new version. if variable is used, multiple ?newVersion are retrieved leading to multiple updates. TODO: fix this
                <<?subjectToUpdate ?predicateToUpdate ?newValue>> citing:valid_from ?newVersion ;
                                                                        citing:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime.
            }
            where {
                # business logic - rows to update as nested select statement
                {%s
                    
                    
                }
                bind('%s' as ?newValue). #new Value
                # versioning
                <<?subjectToUpdate ?predicateToUpdate ?objectToUpdate>> citing:valid_until ?valid_until . 
                BIND(xsd:dateTime(NOW()) AS ?newVersion). 
                filter(?valid_until = "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime)
                filter(?newValue != ?objectToUpdate) # nothing should be changed if old and new value are the same   
            }
        """
        statement = statement % (select_statement, new_value)
        self.sparql_post.setQuery(statement)
        #self.sparql_post.addExtraURITag('pub','http://ontology.ontotext.com/taxonomy/')
        result = self.sparql_post.query()
        print("%s rows updated" % result)

    def delete_triples(self, triple):
        sparql = SPARQLWrapper('http://192.168.0.242:7200/repositories/DataCitation/statements')

        sparql.setHTTPAuth(DIGEST)
        #sparql.setCredentials("dataCitation", "datacitation")
        sparql.setMethod(POST)


        sparql.setQuery("""
        PREFIX pub: <http://ontology.ontotext.com/taxonomy/>

        delete {
            <http://ontology.ontotext.com/resource/tsk9hdnas934> pub:occupation "Cook".
        }
        """)

        results = sparql.query()
        print(results.response.read())



    def insert_triple(self, triple, initNs=None): #, triple, initNs):
        init_ns_nodes = {}
        if initNs is not None:
            init_ns_nodes = _init_ns_to_nodes(initNs)

        self.graphDBStore.method = 'POST'
        ng = Graph(self.graphDBStore)

        ng.add(triple)
        #results = self.graphDBStore.add(spo=(triple))
        #self.graphDBStore.add( (URIRef("http://ontology.ontotext.com/resource/tsk9hdnas934"), FOAF.nick, FOAF.Person))

