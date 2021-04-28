import os
from urllib.error import URLError
import re
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON, Wrapper
from rdflib.term import Literal
import pandas as pd
from datetime import timezone, tzinfo, timedelta, datetime


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


def citation_prefixes(prefixes: dict or str) -> str:
    """
    Extends the given prefixes by citing: <http://ontology.ontotext.com/citing/>
    and xsd: <http://www.w3.org/2001/XMLSchema#>. While citing is reserved and cannot be overwritten by a user prefix
    xsd will be overwritten if a prefix 'xsd' exists in 'prefixes'.
    :param prefixes:
    :return:
    """
    error_message = 'The prefix "citing" is reserved. Please choose another one.'
    prefix_citing = 'PREFIX citing: <https://github.com/GreenfishK/DataCitation/citing/>'
    prefix_xsd = 'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>'

    if isinstance(prefixes, dict):
        sparql_prefixes = prefixes_to_sparql(prefixes)
        if "citing" in prefixes:
            raise ReservedPrefixError(error_message)
        if "xsd" in prefixes:
            citation_prefixes = prefix_citing + "\n"
        else:
            citation_prefixes = prefix_citing + "\n" + prefix_xsd + "\n"
        return sparql_prefixes + "\n" + citation_prefixes

    if isinstance(prefixes, str):
        sparql_prefixes = prefixes
        if prefixes.find("citing:") > -1:
            raise ReservedPrefixError(error_message)
        if prefixes.find("xsd:") > -1:
            citation_prefixes = prefix_citing + "\n"
        else:
            citation_prefixes = prefix_citing + "\n" + prefix_xsd + "\n"
        return sparql_prefixes + "\n" + citation_prefixes


def split_prefixes_query(query: str = None) -> list:
    """
    Separates the prefixes from the actual query and stores either information in self.query and
    self.sparql_prefixes respectively.

    :param query: A query string with or without prefixes
    :return: A list with the prefixes as the first element and the actual query string as the second element.
    """
    pattern = "PREFIX\\s*[a-zA-Z0-9]*:\\s*<.*>\\s*"

    prefixes_list = re.findall(pattern, query, re.MULTILINE)
    prefixes = ''.join(prefixes_list)
    query_without_prefixes = re.sub(pattern, "", query, re.MULTILINE)

    return [prefixes, query_without_prefixes]


def _template_path(template_rel_path: str):
    return os.path.join(os.path.dirname(__file__), template_rel_path)


def _citation_timestamp_format(citation_timestamp: datetime) -> str:
    """

    :param citation_timestamp: must be provided including the timezone
    :return:
    """
    return citation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" + citation_timestamp.strftime("%z")[3:5]


class ReservedPrefixError(Exception):
    pass


class TripleStoreEngine:
    """

    """

    class Credentials:

        def __init__(self, user_name: str, pw: str):
            self.user_name = user_name
            self.pw = pw

    def __init__(self, query_endpoint, update_endpoint, credentials: Credentials = None):
        """
        During initialization a few queries are executed against the RDF* store to test connection but also whether
        the RDF* store in fact supports the 'star' extension. During the execution a side effect may occur and
        additional triples may be added by the RDF* store. These triples are pure meta data triples and reflect
        classes and properties (like rdf:type and rdfs:subPropertyOf) of RDF itself. This happens due to a new prefix,
        namely, citing: <https://github.com/GreenfishK/DataCitation/citing/>' which is used in the write statements.
        Upon execution, this prefix gets embedded into the RDF class hierarchy by the RDF store, thus, new triples
        are written to the store.

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
            self.sparql_get.setQuery(open(self._template_location +
                                          "/test_connection/test_connection_select.txt", "r").read())

            insert_statement = open(self._template_location +
                                    "/test_connection/test_connection_insert.txt", "r").read()
            self.sparql_post.setQuery(insert_statement)
            print(insert_statement)
            self.sparql_post.query()

            delete_statement = open(self._template_location +
                                    "/test_connection/test_connection_delete.txt", "r").read()
            self.sparql_post.setQuery(delete_statement)
            self.sparql_post.query()

        except URLError as e:
            print("No connection to the RDF* store could be established. Check whether your RDF* store is running.")
            raise

        try:
            test_prefixes = citation_prefixes("")
            template = open(self._template_location +
                            "/test_connection/test_connection_nested_select.txt", "r").read()
            select_statement = template.format(test_prefixes)
            self.sparql_get.setQuery(select_statement)
            self.sparql_get.query()

            template = open(self._template_location +
                            "/test_connection/test_connection_nested_insert.txt", "r").read()
            insert_statement = template.format(test_prefixes)
            self.sparql_post.setQuery(insert_statement)
            self.sparql_post.query()

            template = open(self._template_location +
                            "/test_connection/test_connection_nested_delete.txt", "r").read()
            delete_statement = template.format(test_prefixes)
            self.sparql_post.setQuery(delete_statement)
            self.sparql_post.query()

        except Exception as e:
            print("Your RDF store might not support the 'star' extension. Make sure that it is a RDF* store.")
            raise

        print("Connection to RDF query and update endpoints "
              "{0} and {1} established".format(query_endpoint, update_endpoint))

    def reset_all_versions(self):
        """
        Delete all triples with citing:valid_from and citing:valid_until as predicate.

        :return:
        """

        template = open(self._template_location + "/reset_all_versions.txt", "r").read()
        delete_statement = template.format(citation_prefixes(""))
        self.sparql_post.setQuery(delete_statement)
        self.sparql_post.query()
        print("All annotations have been removed.")

    def version_all_rows(self, initial_timestamp: datetime):
        """
        Version all rows with the current timestamp.
        :param initial_timestamp: must also include the timezone

        :return:
        """

        version_timestamp = _citation_timestamp_format(initial_timestamp)
        template = open(self._template_location + "/version_all_rows.txt", "r").read()
        final_prefixes = citation_prefixes("")
        update_statement = template.format(final_prefixes, version_timestamp)
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

    def update(self, select_statement, new_value):
        """
        Updates all objects that are provided within the select_statement with new_value.
        The caller must bind the subject, predicate and object of the triples to update to ?subjectToUpdate,
        ?predicateToUpdate and ?objectToUpdate respectively. The result of this query must include only existing
        triples. Only the most recent triples (those with
        citing_valid_until = "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime) will be updated.

        The select_statement has to be provided in the following form:
            PREFIXES

            select  ?subjectToUpdate  ?predicateToUpdate ?objectToUpdate {
            <triple statements>

            # Inputs to provide
            bind(?subject as ?subjectToUpdate)
            bind(?predicate as ?predicateToUpdate)
            bind(?object as ?objectToUpdate)
            }

        Example:
            PREFIX pub: <http://ontology.ontotext.com/taxonomy/>
            PREFIX publishing: <http://ontology.ontotext.com/publishing#>

            select distinct ?subjectToUpdate  ?predicateToUpdate ?objectToUpdate {
            ?mention publishing:hasInstance ?person .
            ?document publishing:containsMention ?mention .
            ?person pub:memberOfPoliticalParty ?member .
            ?person pub:preferredLabel ?personLabel .
            ?member pub:hasValue ?party .
            ?party pub:preferredLabel ?party_label
            filter(?personLabel = "Judy Chu"@en)

            # Inputs to provide
            bind(?member as ?subjectToUpdate)
            bind(pub:memberOfPoliticalParty as ?predicateToUpdate)
            bind(?party as ?objectToUpdate)
            }

        :param select_statement: set of triples to update.
        :param new_value: The new value which replaces the objects of the select statement's returned triples
        :return:
        """

        query_prefixes, query = split_prefixes_query(select_statement)
        final_prefixes = citation_prefixes(query_prefixes)
        template = open(self._template_location + "/update_data.txt", "r").read()
        update_statement = template.format(final_prefixes, query, new_value)
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
            sparql_prefixes = citation_prefixes(prefixes)

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
            sparql_prefixes = citation_prefixes(prefixes)
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
        sparql_prefixes = citation_prefixes(prefixes)
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
