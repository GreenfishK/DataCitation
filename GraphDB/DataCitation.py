from rdflib.plugins.sparql import prepareQuery
from rdflib.term import Node

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


def prefixes_to_sparql(prefixes):
    sparql_prefixes = ""
    for key, value in prefixes.items():
        sparql_prefixes += "PREFIX " + key + ":" + "<" + value + "> \n"
    return sparql_prefixes


class DataVersioning:
    """

    """

    def __init__(self, query_endpoint, update_endpoint, prefixes=None, credentials=None):
        """

        :param query_endpoint:
        :param update_endpoint:
        :param prefixes:
        :param credentials:
        """

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
        """

        :return:
        """

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

    def get_triples_to_update(self, statement, prefixes):
        """
        :param prefixes: aliases in SPARQL for URIs. Need to be passed as a dict
        :param statement: a select statement returning a set of triples where the object should be updated. The
        returned variables must be as follows: ?subjectToUpdate, ?predicateToUpdate, ?objectToUpdate
        :return: a set of triples in JSON format where the object should be updated.
        """

        sparql_prefixes = prefixes_to_sparql(prefixes)
        statement = sparql_prefixes + statement
        self.sparql_get.setQuery(statement)
        result = self.sparql_get.query()
        result.print_results()

        return result

    def update(self, select_statement, new_value, prefixes):
        """
        :param select_statement:
        :param new_value:
        :param prefixes:
        :return:
        """

        statement = """
            # prefixes
            %s
            
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
                # business logic - rows to update as select statement
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
        sparql_prefixes = prefixes_to_sparql(prefixes)
        statement = statement % (sparql_prefixes, select_statement, new_value)
        self.sparql_post.setQuery(statement)
        result = self.sparql_post.query()

        print("%s rows updated" % result)

    def insert_triple(self, triple, prefixes):
        """

        :param triple:
        :param prefixes:
        :return:
        """

        statement = """
        insert {{
            {0} {1} {2}.
            <<{0} {1} {2}>>  citing:valid_from ?newVersion.
            <<{0} {1} {2}>>  citing:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime.
        }}
        where {{
            BIND(xsd:dateTime(NOW()) AS ?newVersion). 
        }}
        """
        sparql_prefixes = prefixes_to_sparql(prefixes)
        statement = sparql_prefixes + statement

        if type(triple[0]) == Literal:
            s = "'" + triple[0] + "'"
        else:
            s = triple[0]
        if type(triple[1]) == Literal:
            p = "'" + triple[1] + "'"
        else:
            p = triple[1]
        if type(triple[2]) == Literal:
            o = "'" + triple[2] + "'"
        else:
            o = triple[2]

        statement = statement.format(s,p,o)
        self.sparql_post.setQuery(statement)
        result = self.sparql_post.query()
        return result



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





