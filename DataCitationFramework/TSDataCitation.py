from prettytable import PrettyTable
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON, Wrapper
from rdflib.term import URIRef, Literal, Variable
import rdflib.plugins.sparql.parser as parser
import pyparsing
import rdflib.plugins.sparql.algebra as algebra
from nested_lookup import nested_lookup, get_all_keys
import pandas as pd
import re
from datetime import datetime, timedelta, timezone
import hashlib


def _prefixes_to_sparql(prefixes):
    sparql_prefixes = ""
    for key, value in prefixes.items():
        sparql_prefixes += "PREFIX " + key + ":" + "<" + value + "> \n"
    return sparql_prefixes


def _extend_query_with_sort_operation(query, variables: list = [], colored: bool = False):
    if colored:
        sort_extension = "\x1b[36m" + "order by " + ' '.join(variables) + "\x1b[0m"
    else:
        sort_extension = "order by " + ' '.join(variables)

    sorted_query = """
{0}
{1}
    """

    if query is not None:
        sorted_query = sorted_query.format(query, sort_extension)

    return sorted_query


def _query_triples(query, sparql_prefixes: str = None) -> list:
    """
    Takes a query and transforms it into a result set with three columns: s, p, o. This result set includes all
    stored triples connected to the result set of the input query.
    :return: transformed result set with columns: s, p, o
    """

    statement = """
    {0} 

    {1}
    """

    if sparql_prefixes:
        statement = statement.format(sparql_prefixes, query)
    else:
        statement = statement.format("", query)

    pattern = re.compile('([<?]\S*|".*"\S*)\s(\S*|".*"\S*)\s*(\S*|".*"\S*)\s*\.') # only explicit triples
    # TODO: also consider triples like "?a ?b ?c; ?b ?c." and "?a ?b / ?c ?d"
    unsorted_triples = []
    for t in re.findall(pattern, query):
        triple_string = t[0] + " " + t[1] + " " + t[2]
        unsorted_triples.append(triple_string)
    sorted_triples = sorted(unsorted_triples, reverse=False)

    """q_desc = parser.parseQuery(statement)
    q_algebra = algebra.translateQuery(q_desc).algebra
    triples = nested_lookup('triples', q_algebra)

    n3_triples = []
    for triple_list in triples:
        for triple in triple_list:
            t = triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3()
            n3_triples.append(t)
    """
    return sorted_triples


def _intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


class Query:

    def __init__(self, query: str, prefixes: dict = None):
        """

        :param query: The SPARQL select statement that is used to retrieve the data set for citing
        :param prefixes: prefixes used in the SPARQL query
        """

        self.query = query
        self.normalized_query = query
        self.normalized_query_timestamped = query
        if prefixes is not None:
            self.sparql_prefixes = _prefixes_to_sparql(prefixes)
        else:
            self.sparql_prefixes = ""
        q_desc = parser.parseQuery(self.sparql_prefixes + " " + query)
        self.query_algebra = algebra.translateQuery(q_desc).algebra
        self.variables = self._query_variables(query, self.sparql_prefixes)
        # self.triples = _query_triples(query, self.sparql_prefixes) # include this?
        self.query_pid = None
        # self.query_result = None # include this?

    def _query_variables(self, query, sparql_prefixes: str = None) -> list:
        """
        The query must be a valid query including prefixes. They can be already embedded in the query or will
        be embedded by providing them separately with the 'prefix' parameter.
        The query algebra is searched for "PV". There can be more than one PV-Nodes containing the select-clause
        variables within a list. However, each of these lists enumerates the same variables only the first list
        will be selected and returned.

        :param query: The query which will be searched for variables
        :param prefixes: The prefixes used in the query.
        :return: a list of variables used in the query
        """

        query_triples = nested_lookup('triples', self.query_algebra)
        triple_variables = []
        for triple_list in query_triples:
            for triple in triple_list:
                if isinstance(triple[0], Variable):
                    triple_variables.append(triple[0])
                if isinstance(triple[1], Variable):
                    triple_variables.append(triple[1])
                if isinstance(triple[2], Variable):
                    triple_variables.append(triple[2])

        distinct_triple_variables = list(dict.fromkeys(triple_variables))
        query_variables = nested_lookup('PV', self.query_algebra)[0]
        variables = _intersection(distinct_triple_variables, query_variables)

        """q_desc = parser.parseQuery(sparql_prefixes + " " + query)
        q_algebra = algebra.translateQuery(q_desc).algebra

        variables = []
        get_vars = lambda x, l: [l.append(a) for a in x if isinstance(a, Variable)]
        algebra.traverse(q_algebra, lambda s: get_vars(s, variables) if isinstance(s, set) else None)
        variables = list(dict.fromkeys(variables))"""
        return distinct_triple_variables

    def normalize_query_tree(self):
        """
        Normalizes the query by computing its algebra first. Most of the ambiguities are implicitly solved by the query
        algebra as different ways of writing a query usually boil down to one algebra. For case where normalization is
        still needed, the query algebra will be updated.
        :return: normalized query tree object
        """

        """
        #3
        In case of an asterisk in the select-clause, all variables will be explicitly mentioned 
        and ordered alphabetically.
        """
        pass
        # Implicitly solved by query algebra

        """
        #1
        A where clause will always be inserted
        """
        pass
        # Implicitly solved by query algebra

        """
        #5
        Triple statements will be ordered alphabetically by subject, predicate, object.

        The triple order is already unambiguous in the query algebra. The only requirement is
        that the variables in the select clause are in an unambiguous order. E.g. ?a ?b ?c ...
        """
        pass
        # Implicitly solved by query algebra

        """
        #7
        Variables in the query tree will be replaced by letters from the alphabet. For each variable 
        a letter from the alphabet will be assigned starting with 'a' and continuing in alphabetic order. 
        Two letters will be used  and combinations of letters will be assigned should there be more than 26 variables. 
        There is no need to sort the variables in the query tree as this is implicitly solved by the algebra. Variables
        are sorted by their bindings where the ones with the most bindings come first.
        """

        int_norm_var = 'a'
        variables_mapping = {}
        for i, v in enumerate(self.variables):
            next_norm_var = chr(ord(int_norm_var) + i)
            variables_mapping.update({v: next_norm_var})

        # replace variables with normalized variables (letters from the alphabet)
        algebra.traverse(self.query_algebra,
                         lambda var: variables_mapping.get(var) if isinstance(var, Variable) else None
                         )

        # identify _var sets and sort the variables inside
        norm_vars = lambda x: [variables_mapping[a] for a in x]
        algebra.traverse(self.query_algebra,
                         lambda s: sorted(norm_vars(s)) if isinstance(s, set) else None
                         )

        # Sort variables in select statement
        algebra.traverse(self.query_algebra,
                         lambda l: sorted(l) if isinstance(l, list) else None
                         )
        return self.query_algebra

    def extend_query_with_timestamp(self, timestamp, colored: bool = False):
        """
        :param timestamp:
        :param colored: If colored is true, the injected strings within the statement template will be colored.
        Use this parameter only for presentation purpose as the code for color encoding will malform the SPARQL query!
        :return:
        """

        if colored:
            template = """
# prefixes
\x1b[36m{0}\x1b[0m
    
Select \x1b[31m{1}\x1b[0m where {{
    # original query comes here
    {{ 
        \x1b[32m{2}\x1b[0m
    }}
    # version timestamp
    bind("\x1b[34m{3}\x1b[0m"^^xsd:dateTime as ?TimeOfCiting) 

    # data versioning query extension
    \x1b[35m{4}\x1b[0m

}}
    """
        else:
            template = """
# prefixes
{0} 

Select {1} where {{
    # original query comes here
    {{ 
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

        # columns of result set. Will be as in normalized query
        variables_injection_string = ""
        normalized_variables = self._query_variables(self.normalized_query, self.sparql_prefixes)
        for v in normalized_variables:
            variables_injection_string += v.n3() + " "

        # Query extensions for versioning injection
        normalized_triples = _query_triples(self.normalized_query, self.sparql_prefixes)

        versioning_query_extensions_template = """
        <<{0}>> citing:valid_from {1}.
        <<{0}>> citing:valid_until {2}.
        filter({1} <= ?TimeOfCiting && ?TimeOfCiting < {2})
    """

        versioning_query_extensions = ""
        for i, triple in enumerate(normalized_triples):
            v = versioning_query_extensions_template
            versioning_query_extensions += v.format(triple, "?valid_from_" + str(i), "?valid_until_" + str(i))

        # Formatting and styling select statement
        indent = '        '
        normalized_query_formatted = self.normalized_query.replace('\n', '\n' + indent)

        self.normalized_query_timestamped = template.format(self.sparql_prefixes,
                                                            variables_injection_string,
                                                            normalized_query_formatted,
                                                            timestamp,
                                                            versioning_query_extensions)

        return self.normalized_query_timestamped

    # TODO: implement this
    def generate_query_pid(self):
        self.query_pid = 12345
        return self.query_pid

    def compute_checksum(self, query_or_result):
        """
        :param input: can be either a result set or a query object tree. In case of a query object tree
        the hash value will be computed of the object string.
        :param query_or_result:
        :return: the hash value of either the query or query result
        """

        if query_or_result == "query":
            query_algebra_string = str(self.query_algebra)
            checksum = hashlib.sha256()
            checksum.update(str.encode(query_algebra_string))

            return checksum.hexdigest()
        if query_or_result == "result":
            pass


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
        self.query_object = None
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
        query = Query(select_statement, prefixes)
        statement = """
            # prefixes
            {0}

            
            select ?subjectToUpdate ?predicateToUpdate ?objectToUpdate ?newValue
            where {{
                # business logic - rows to update as select statement
                {{
                {1}
                
                
                }}
                
                bind('{2}' as ?newValue). #new Value
                
                # versioning
                <<?subjectToUpdate ?predicateToUpdate ?objectToUpdate>> citing:valid_until ?valid_until . 
                BIND(xsd:dateTime(NOW()) AS ?newVersion). 
                filter(?valid_until = "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime)
                filter(?newValue != ?objectToUpdate) # nothing should be changed if old and new value are the same   
            }}
        """

        statement = statement.format(query.sparql_prefixes, query.query, new_value)
        self.sparql_get.setQuery(statement)
        result = self.sparql_get.query()
        result.print_results()

        return result

    def get_data(self, select_statement, prefixes: dict = None, is_timestamped: bool = False):
        """
        :param select_statement:
        :param prefixes:
        :return:
        """

        if is_timestamped:
            self.sparql_get.setQuery(select_statement)
            result = self.sparql_get.query()
            df = _QueryResult_to_dataframe(result)
        else:
            vieTZObject = timezone(timedelta(hours=2))
            current_timestamp = datetime.now(vieTZObject)
            df = self.get_data_at_timestamp(select_statement, current_timestamp, prefixes)
        return df

    def get_data_at_timestamp(self, select_statement, timestamp, prefixes: dict = None):
        """

        :param select_statement:
        :param timestamp: timestamp of the snapshot. The timestamp must be in format: yyyy-MM-ddTHH:mm:ss.SSS+ZZ:ZZ
        :param prefixes:
        :return: Dataframe object
        """
        query = Query(select_statement, prefixes)
        timestamped_query = query.extend_query_with_timestamp(timestamp)

        self.sparql_get.setQuery(timestamped_query)
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
        query = Query(select_statement, prefixes)
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

        statement = statement % (query.sparql_prefixes, query.query, new_value)
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
            sparql_prefixes = _prefixes_to_sparql(prefixes)
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
            sparql_prefixes = _prefixes_to_sparql(prefixes)
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
        sparql_prefixes = _prefixes_to_sparql(prefixes)
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

    def cite(self, select_statement, result_set_description):
        citation_text = ""
        query = Query(select_statement)
        normalized_query = query.normalize_query_tree()

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
        query_PID = query.generate_query_PID()

        # execute query

        # compute hash value of result set

        # store: query PID, query checksum, query, normalized query, version timestamp, execution timestamp, result
        # result set check sum, result set description

        return citation_text

        # embed query timestamp (max valid_from of dataset)
