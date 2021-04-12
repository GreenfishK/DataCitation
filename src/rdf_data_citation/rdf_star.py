import os
from urllib.error import URLError
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON, Wrapper
from rdflib.term import Literal
import pandas as pd


def _to_df(result: Wrapper.QueryResult) -> pd.DataFrame:
    """

    :param result:
    :return: Dataframe
    """
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

    def format_value(res_value):
        value = res_value["value"]
        lang = res_value.get("xml:lang", None)
        datatype = res_value.get("datatype", None)
        if lang is not None:
            value += "@" + lang
        if datatype is not None:
            value += " [" + datatype + "]"
        return value

    results = result.convert()

    column_names = []
    for var in results["head"]["vars"]:
        column_names.append(var)
    df = pd.DataFrame(columns=column_names)

    values = []
    for r in results["results"]["bindings"]:
        row = []
        for var in results["head"]["vars"]:
            result_value = format_value(r[var])
            row.append(result_value)
        values.append(row)
    df = df.append(pd.DataFrame(values, columns=df.columns))
    return df


def prefixes_to_sparql(prefixes: dict) -> str:
    """
    Converts a dict of prefixes to a string with SPARQL syntax for prefixes
    :param prefixes:
    :return: SPARQL prefixes as string
    """
    if prefixes is None:
        return ""

    sparql_prefixes = ""
    for key, value in prefixes.items():
        sparql_prefixes += "PREFIX {0}: <{1}> \n".format(key, value)
    return sparql_prefixes


def _template_path(template_rel_path: str):
    return os.path.join(os.path.dirname(__file__), template_rel_path)


class TripleStoreEngine:
    """

    """

    class Credentials:

        def __init__(self, user_name: str, pw: str):
            self.user_name = user_name
            self.pw = pw

    def __init__(self, query_endpoint, update_endpoint, credentials: Credentials = None):
        """
        :param query_endpoint: URL for executing read/select statements on the RDF store. In GRAPHDB this URL can be
        looked up under "Setup --> Repositories --> Link icon"
        :param update_endpoint: URL for executing write statements on the RDF store. Its URL is an extension of
        query_endpoint: "query_endpoint/statements"
        :param credentials: The user name and password for the remote RDF store
        """

        # Parameters
        self.sparql_get = SPARQLWrapper(query_endpoint)
        self.sparql_post = SPARQLWrapper(update_endpoint)
        self.credentials = credentials

        # Settings
        self._template_location = _template_path("templates/rdf_star_store")
        self.sparql_post.setHTTPAuth(DIGEST)
        self.sparql_post.setMethod(POST)

        self.sparql_get.setHTTPAuth(DIGEST)
        self.sparql_get.setMethod(GET)
        self.sparql_get.setReturnFormat(JSON)

        if self.credentials is not None:
            self.sparql_post.setCredentials(credentials.user_name, credentials.pw)
            self.sparql_get.setCredentials(credentials.user_name, credentials.pw)

        # Test connection. Execute one read and one write statement
        try:
            self.sparql_get.setQuery(open(self._template_location + "/test_connection_select.txt", "r").read())
            result = self.sparql_get.query()
            print(result)

            insert_statement = open(self._template_location + "/test_connection_insert.txt", "r").read()
            self.sparql_post.setQuery(insert_statement)
            self.sparql_post.query()

            delete_statement = open(self._template_location + "/test_connection_delete.txt", "r").read()
            self.sparql_post.setQuery(delete_statement)
            self.sparql_post.query()
            print("Connection established")
        except URLError as e:
            print("No connection to the RDF* store could be established. check whether your RDF* store is running.")
            raise

    def reset_all_versions(self):
        """
        Delete all triples with citing:valid_from and citing:valid_until as predicate.

        :return:
        """

        delete_statement = open(self._template_location + "/reset_all_versions.txt", "r").read()

        self.sparql_post.setQuery(delete_statement)
        self.sparql_post.query()
        print("All annotations have been removed.")

    def version_all_rows(self):
        """
        Version all rows with the current timestamp.

        :return:
        """

        update_statement = open(self._template_location + "/version_all_rows.txt", "r").read()

        self.sparql_post.setQuery(update_statement)
        self.sparql_post.query()
        print("All rows have been annotated with the current timestamp")

    def get_data(self, select_statement) -> pd.DataFrame:
        """
        Executes the SPARQL select statement and returns a result set.

        snapshot of the date as of "timestamp".
        :param select_statement:
        :return:
        """

        query = select_statement
        self.sparql_get.setQuery(query)
        result = self.sparql_get.query()
        df = _to_df(result)
        return df

    def update(self, select_statement, new_value, prefixes: dict):
        """
        All objects from the select statement's returned triples will be updated with the new value.

        :param select_statement: set of triples to update.
        :param new_value: The new value which replaces the objects of the select statement's returned triples
        :param prefixes: aliases of provided URIs which are resolved to these URIs during the execution.
        :return:
        """

        update_statement = open(self._template_location + "/get_data.txt", "r").read()

        update_statement = update_statement.format(prefixes, select_statement, new_value)
        self.sparql_post.setQuery(update_statement)
        result = self.sparql_post.query()

        print("{0} rows updated".format(result))

    def insert_triple(self, triple, prefixes: dict):
        """
        Inserts a new triple into the RDF* store and two additional (nested) triples labeling the newly inserted triple
        with a valid_from and valid_until date.

        :param triple:
        :param prefixes:
        :return:
        """

        statement = open(self._template_location + "/insert_triple.txt", "r").read()

        sparql_prefixes = ""
        if prefixes:
            sparql_prefixes = prefixes_to_sparql(prefixes)

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

        statement = statement.format(sparql_prefixes, s, p, o)
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

        statement = open(self._template_location + "/outdate_triples.txt", "r").read()
        sparql_prefixes = ""
        if prefixes:
            sparql_prefixes = prefixes_to_sparql(prefixes)
        statement = statement.format(sparql_prefixes, select_statement)
        self.sparql_post.setQuery(statement)
        result = self.sparql_post.query()

        print("{0} rows outdated".format(result))

    def _delete_triples(self, triple, prefixes):
        """
        Deletes the triples and its version annotations from the history. Should be used with care
        as it is most of times not intended to delete triples but to outdate them. This way they will
        still appear in the history and will not appear when querying more recent versions.

        :param triple:
        :param prefixes:
        :return:
        """

        statement = open(self._template_location + "/_delete_triples.txt", "r").read()
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
