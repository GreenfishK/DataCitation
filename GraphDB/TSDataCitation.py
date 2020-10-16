from prettytable import PrettyTable
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON, Wrapper
from rdflib.term import URIRef, Literal, Variable
from rdflib.plugins.sparql.parser import parseQuery
import rdflib.plugins.sparql.algebra as algebra
from nested_lookup import nested_lookup
import pandas as pd


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


def _get_all_triples_from_stmt(query, prefixes):
    """
    Takes a query and transforms it into a result set with three columns: s, p, o. This result set includes all
    stored triples connected to the result set of the input query.
    :return: transformed result set with columns: s, p, o
    """

    statement = """
    {0} 
    
    {1}
    """

    statement = statement.format(prefixes, query)

    q_desc = parseQuery(statement)
    # q_algebra = algebra.translateQuery(q_desc) is a Query object. Query object has prologue attribute which holds
    # a method called "bind" to bind prefixes to URIs
    q_algebra = algebra.translateQuery(q_desc).algebra
    triples = nested_lookup('triples', q_algebra)
    return triples


def _get_all_variables_from_stmt(query, prefixes):
    statement = """
    {0} 

    {1}
    """

    statement = statement.format(prefixes, query)

    q_desc = parseQuery(statement)
    q_algebra = algebra.translateQuery(q_desc).algebra
    variables = nested_lookup('_vars', q_algebra)

    var_sets = []
    for var in variables:
        if var not in var_sets:
            var_sets.append(var)

    variables = []
    for s in var_sets:
        for var in s:
            if isinstance(var, Variable):
                variables.append(var)

    return variables


# TODO: implement this
def normalize_query(query):
    normalized_query = query
    return normalized_query


# TODO: implement this
def extend_query_with_sort_operation(query):
    sorted_query = query
    return sorted_query


# TODO: implement this
def generate_query_PID(normalized_query):
    query_PID = 12345
    return query_PID


def extend_query_with_timestamp(select_statement, timestamp, prefixes):
    statement = """
    # prefixes
    {0} 
    
    Select {1}  where {{
        {{ 
        # original query
        {2}
        }}
        # version timestamp
        bind("{3}"^^xsd:dateTime as ?TimeOfCiting) 

        # data versioning query extension
        {4} 

    }}
    """

    # Prefixes, and timestamp injection
    timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" + timestamp.strftime("%z")[3:5]

    sparql_prefixes = ""
    if prefixes:
        sparql_prefixes = prefixes_to_sparql(prefixes)

    # columns of result set. Will be as in original query
    variables = _get_all_variables_from_stmt(select_statement, sparql_prefixes)
    variables_injection_string = ""
    for v in variables:
        variables_injection_string += v.n3() + " "

    # Query extensions for versioning injection
    triples = _get_all_triples_from_stmt(select_statement, sparql_prefixes)

    versioning_query_extensions_template = """
            <<{0}>> citing:valid_from {1}.
            <<{0}>> citing:valid_until {2}.
            filter({1} <= ?TimeOfCiting && ?TimeOfCiting < {2}) # get data at a certain point in time

            """
    versioning_query_extensions = ""
    i = 0
    for triple_list in triples:
        for triple in triple_list:
            t = triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3()
            v = versioning_query_extensions_template
            versioning_query_extensions += v.format(t, "?valid_from_" + str(i), "?valid_until_" + str(i))
            i += 1

    statement = statement.format(sparql_prefixes,
                                 variables_injection_string,
                                 select_statement,
                                 timestamp,
                                 versioning_query_extensions)

    return statement


def _get_prettyprint_string_sparql_var_result(result):
    value = result["value"]
    lang = result.get("xml:lang", None)
    datatype = result.get("datatype", None)
    if lang is not None:
        value += "@"+lang
    if datatype is not None:
        value += " ["+datatype+"]"
    return value


def _QueryResult_to_dataframe(result: Wrapper.QueryResult) -> pd.DataFrame:
    """

    :param result:
    :return: Dataframe
    """
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

    results = result.convert()

    column_names = []
    for var in results["head"]["vars"]:
        column_names.append(var)
    df = pd.DataFrame(columns=column_names)

    values = []
    for r in results["results"]["bindings"]:
        row = []
        for var in results["head"]["vars"]:
            result_value = _get_prettyprint_string_sparql_var_result(r[var])
            row.append(result_value)
        values.append(row)
    df = df.append(pd.DataFrame(values, columns=df.columns))
    return df


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

    def get_triples_to_update(self, select_statement, new_value, prefixes: dict):
        """
        :param new_value: The new value to override the select statement's objects.
        :param prefixes: aliases in SPARQL for URIs. Need to be passed as a dict
        :param select_statement: a select statement returning a set of triples where the object should be updated. The
        returned variables must be as follows: ?subjectToUpdate, ?predicateToUpdate, ?objectToUpdate
        :return: a set of triples in JSON format where the object should be updated.
        """

        statement = """
            # prefixes
            %s

            
            select ?subjectToUpdate ?predicateToUpdate ?objectToUpdate ?newValue
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
        sparql_prefixes = ""
        if prefixes:
            sparql_prefixes = prefixes_to_sparql(prefixes)
        statement = statement % (sparql_prefixes, select_statement, new_value)
        self.sparql_get.setQuery(statement)
        result = self.sparql_get.query()
        result.print_results()

        return result

    def get_data(self, select_statement, prefixes: dict):
        """
        :param select_statement:
        :param prefixes:
        :return:
        """

        statement = """
            # prefixes
            {0} 

            {1}
            """

        sparql_prefixes = ""
        if prefixes:
            sparql_prefixes = prefixes_to_sparql(prefixes)

        statement = statement.format(sparql_prefixes, select_statement)
        self.sparql_get.setQuery(statement)
        result = self.sparql_get.query()
        df = _QueryResult_to_dataframe(result)
        return df

    def get_data_at_timestamp(self, select_statement, timestamp, prefixes: dict):
        """

        :param select_statement:
        :param timestamp: timestamp of the snapshot. The timestamp must be in format: yyyy-MM-ddTHH:mm:ss.SSS+ZZ:ZZ
        :param prefixes:
        :return: Dataframe object
        """

        ext_query = extend_query_with_timestamp(select_statement, timestamp, prefixes)

        self.sparql_get.setQuery(ext_query)
        result = self.sparql_get.query()
        df = _QueryResult_to_dataframe(result)
        return df

    def update(self, select_statement, new_value, prefixes: dict):
        """
        All objects from the select statement's returned triples will be updated with the new value.

        :param select_statement: set of triples to update.
        :param new_value: The new value which replaces the objects of the select statement's returned triples
        :param prefixes: aliases of provided URIs which are resolved to these URIs during the execution.
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
        sparql_prefixes = ""
        if prefixes:
            sparql_prefixes = prefixes_to_sparql(prefixes)
        statement = statement % (sparql_prefixes, select_statement, new_value)
        self.sparql_post.setQuery(statement)
        result = self.sparql_post.query()

        print("%s rows updated" % result)

    def insert_triple(self, triple, prefixes: dict):
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

        sparql_prefixes = ""
        if prefixes:
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

        statement = statement.format(s, p, o)
        self.sparql_post.setQuery(statement)
        result = self.sparql_post.query()
        return result

    def outdate_triples(self, select_statement, prefixes):
        """
        Triples provided as input will be outdated and marked with an valid_until timestamp. Thus, they will
        not appear in result sets queried from the most recent graph version or any other version that came after
        their expiration.
        The triples provided must exist in the triple store.
        :param select_statement:
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
                # outdate old triples with date as of now()
                <<?subjectToUpdate ?predicateToUpdate ?objectToUpdate>> citing:valid_until ?newVersion.
            }
            where {
                # business logic - rows to outdate as select statement
                {%s


                }

                # versioning
                <<?subjectToUpdate ?predicateToUpdate ?objectToUpdate>> citing:valid_until ?valid_until . 
                BIND(xsd:dateTime(NOW()) AS ?newVersion). 
                filter(?valid_until = "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime)
            }
        """
        sparql_prefixes = ""
        if prefixes:
            sparql_prefixes = prefixes_to_sparql(prefixes)
        statement = statement % (sparql_prefixes, select_statement)
        self.sparql_post.setQuery(statement)
        result = self.sparql_post.query()

        print("%s rows outdated" % result)

    def _delete_triples(self, triple, prefixes):
        """
        Deletes the triples and its version annotations from the history. Should be used with care
        as it is most of times not intended to delete triples but to outdate them. This way they will
        still appear in the history and will not appear when querying more recent versions.

        :param triple:
        :param prefixes:
        :return:
        """

        statement = """

       delete {{    
            <<?s ?p ?o>>  citing:valid_from ?valid_from.
            <<?s ?p ?o>>  citing:valid_until ?valid_until.
            ?s ?p ?o.
        }} where {{
            bind({0} as ?s)
            bind({1} as ?p)
            bind({2} as ?o)
            <<?s ?p ?o>> citing:valid_from ?valid_from.
            <<?s ?p ?o>> citing:valid_until ?valid_until.
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

        statement = statement.format(s, p, o)
        self.sparql_post.setQuery(statement)
        result = self.sparql_post.query()
        return result

    def compute_checksume(self, type):
        """

        :param type:
        :return:
        """
        pass

    def cite(self, select_statement, result_set_description):
        citation_text = ""
        normalized_query = normalize_query(select_statement)

        # extend query with version timestamp

        # extend query with sort operation

        # compute checksum

        # check query against query store

        # case 1: query exists: get query from query store

        # execute query

        # compute hash value of result set

        # compare hash values

        # generate citation text

        # case 2: query does not exist:

        # generate query PID
        query_PID = generate_query_PID(normalized_query)

        # execute query

        # compute hash value of result set

        # store: query PID, query checksum, query, normalized query, version timestamp, execution timestamp, result
        # result set check sum, result set description

        return citation_text

        # embed query timestamp (max valid_from of dataset)
