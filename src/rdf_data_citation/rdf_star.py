from src.rdf_data_citation.citation_utils import QueryUtils
from src.rdf_data_citation._helper import template_path, config, citation_timestamp_format
from src.rdf_data_citation.prefixes import citation_prefixes, split_prefixes_query
from src.rdf_data_citation.exceptions import NoVersioningMode
from urllib.error import URLError
from enum import Enum
import logging
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON, Wrapper
from rdflib.term import Literal
import pandas as pd
from datetime import datetime


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


class VersioningMode(Enum):
    Q_PERF = 1
    SAVE_MEM = 2


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

        self.sparql_get = SPARQLWrapper(query_endpoint)
        self.sparql_post = SPARQLWrapper(update_endpoint)
        self.credentials = credentials
        self._template_location = template_path("templates/rdf_star_store")
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
        config().set('VERSIONING', 'yn_init_version_all_applied', 'False')
        print("All annotations have been removed.")

    def version_all_rows(self, initial_timestamp: datetime, versioning_mode: VersioningMode = VersioningMode.SAVE_MEM):
        """
        Version all triples with an artificial end date. If the mode is Q_PERF then every triple is additionally
        annotated with a valid_from date where the date is the initial_timestamp provided by the caller.

        :param versioning_mode: The mode to use for versioning your data in the RDF store. The Q_PERF mode takes up
        more storage as for every triple in the RDF store two additional triples are added. In return, querying
        timestamped data is faster. The SAVE_MEM mode only adds one additional triple per triple in the RDF store.
        However, the queries are more time-consuming as additional filters are needed. Make sure to choose the mode
        the better suits your need as the mode gets set only once at the beginning. Every subsequent query that gets
        send to the RDF endpoint using get_data() will also operate in the chosen mode.
        :param initial_timestamp: Timestamp which also must include the timezone. Only relevant for Q_PERF mode.

        :return:
        """

        if config().get('VERSIONING', 'yn_init_version_all_applied') == 'False':
            version_timestamp = citation_timestamp_format(initial_timestamp)
            template = open(self._template_location + "/version_all_rows.txt", "r").read()
            final_prefixes = citation_prefixes("")
            versioning_mode_dir = self._template_location + "/../query_utils/versioning_modes"
            if versioning_mode == VersioningMode.Q_PERF:
                update_statement = template.format(final_prefixes, "<<?s ?p ?o>> citing:valid_from ?currentTimestamp;",
                                                   version_timestamp)

                # Prepare template for query timestamping/versioning
                versioning_mode_template = \
                    open(versioning_mode_dir + "/versioning_query_extensions_q_perf.txt", "r").read()

            else:
                update_statement = template.format(final_prefixes, "", version_timestamp)

                # Prepare template for query timestamping/versioning
                versioning_mode_template = \
                    open(versioning_mode_dir + "/versioning_query_extensions_save_mem.txt", "r").read()

            with open(self._template_location + "/../query_utils/versioning_query_extensions.txt", "w") as vers:
                vers.write(versioning_mode_template)

            self.sparql_post.setQuery(update_statement)
            self.sparql_post.query()
            print("All rows have been annotated with an artificial end date.")
        else:
            print("All rows are already versioned.")

    def get_data(self, select_statement, timestamp: datetime = None, yn_timestamp_query: bool = True) -> pd.DataFrame:
        """
        Executes the SPARQL select statement and returns a result set. If the timestamp is provided the result set
        will be a snapshot of the data as of timestamp. Otherwise, the most recent version of the data will be returned.

        :param yn_timestamp_query: If true, the select statement will be wrapped with the versioning template. This
        also means that the data must have already been versioned using version_all_rows(). Otherwise, the query
        is executed as it is against the RDF store.
        :param timestamp:
        :param select_statement:
        :return:
        """

        if yn_timestamp_query:
            if timestamp is None:
                query_utils = QueryUtils(query=select_statement)
            else:
                query_utils = QueryUtils(query=select_statement, citation_timestamp=timestamp)

            query = query_utils.timestamped_query
            self.sparql_get.setQuery(query)
        else:
            self.sparql_get.setQuery(select_statement)
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

    def outdate_triples(self, select_statement):
        """
        Triples provided as input will be outdated and marked with an valid_until timestamp. Thus, they will
        not appear in result sets queried from the most recent graph version or any other version that came after
        their expiration.
        The triples provided must exist in the triple store. They must be provided as select statement in the following
        form:

        # Prefixes
        <prefixes>

        select ?subjectToOutdate ?predicateToOutdate ?objectToOutdate {
            # Triple staments, filter, ...
            <triple statement>
            <triple statement>
            ...

            bind(<subject> as ?subjectToOutdate)
            bind(<predicate> as ?predicateToOutdate)
            bind(<object> as ?objectToOutdate)

        } order by ?mention

        :param select_statement:
        :return:
        """

        template = open(self._template_location + "/outdate_triples.txt", "r").read()
        query_prefixes, query = split_prefixes_query(select_statement)
        final_prefixes = citation_prefixes(query_prefixes)
        statement = template.format(final_prefixes, query)
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
