from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON, Wrapper
from rdflib.term import Literal, Variable
import pandas as pd
from datetime import datetime, timedelta, timezone
from DataCitationFramework.TSDataCitation import Query, prefixes_to_sparql


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



class SPARQLAPI:
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

    def get_data(self, select_statement, prefixes: dict = None, is_timestamped: bool = False) -> pd.DataFrame:
        """
        :param is_timestamped:
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