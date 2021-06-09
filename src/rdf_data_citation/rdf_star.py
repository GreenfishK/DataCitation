from citation_utils import QueryUtils
from _helper import template_path, citation_timestamp_format
from prefixes import citation_prefixes, split_prefixes_query
from _exceptions import RDFStarNotSupported, NoConnectionToRDFStore, NoVersioningMode, \
    WrongInputFormatException
from urllib.error import URLError
from enum import Enum
import logging
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON, Wrapper
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
        for col in results["head"]["vars"]:
            if col in r:
                result_value = format_value(r[col])
            else:
                result_value = None
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

        self.credentials = credentials
        self._template_location = template_path("templates/rdf_star_store")

        self.sparql_get = SPARQLWrapper(query_endpoint)
        self.sparql_get.setHTTPAuth(DIGEST)
        self.sparql_get.setMethod(GET)
        self.sparql_get.setReturnFormat(JSON)

        self.sparql_get_with_post = SPARQLWrapper(query_endpoint)
        self.sparql_get_with_post.setHTTPAuth(DIGEST)
        self.sparql_get_with_post.setMethod(POST)
        self.sparql_get_with_post.setReturnFormat(JSON)

        self.sparql_post = SPARQLWrapper(update_endpoint)
        self.sparql_post.setHTTPAuth(DIGEST)
        self.sparql_post.setMethod(POST)

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

        except URLError:
            raise NoConnectionToRDFStore("No connection to the RDF* store could be established. "
                                         "Check whether your RDF* store is running.")

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

        except Exception:
            raise RDFStarNotSupported("Your RDF store might not support the 'star' extension. "
                                      "Make sure that it is a RDF* store.")

        logging.info("Connection to RDF query and update endpoints "
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

        logging.info("All annotations have been removed.")

    def version_all_rows(self, initial_timestamp: datetime = None,
                         versioning_mode: VersioningMode = VersioningMode.SAVE_MEM):
        """
        Version all triples with an artificial end date. If the mode is Q_PERF then every triple is additionally
        annotated with a valid_from date where the date is the initial_timestamp provided by the caller.

        :param versioning_mode: The mode to use for versioning your data in the RDF store. The Q_PERF mode takes up
        more storage as for every triple in the RDF store two additional triples are added. In return, querying
        timestamped data is faster. The SAVE_MEM mode only adds one additional metadata triple per data triple
        to the RDF store. However, the queries are more time-consuming as additional filters are needed.
        Make sure to choose the mode the better suits your need as the mode gets set only once at the beginning.
        Every subsequent query that gets send to the RDF endpoint using get_data() will also operate in the chosen mode.
        :param initial_timestamp: Timestamp which also must include the timezone. Only relevant for Q_PERF mode.
        :return:
        """

        final_prefixes = citation_prefixes("")
        versioning_mode_dir1 = self._template_location + "/../rdf_star_store/versioning_modes"
        versioning_mode_dir2 = self._template_location + "/../query_utils/versioning_modes"

        if versioning_mode == VersioningMode.Q_PERF and initial_timestamp is not None:
            version_timestamp = citation_timestamp_format(initial_timestamp)

            versioning_mode_template1 = open(versioning_mode_dir1 + "/version_all_rows_q_perf.txt", "r").read()
            versioning_mode_template2 = \
                open(versioning_mode_dir2 + "/versioning_query_extensions_q_perf.txt", "r").read()
            update_statement = versioning_mode_template1.format(final_prefixes, version_timestamp)
            message = "All rows have been annotated with start date {0} " \
                      "and an artificial end date".format(initial_timestamp)
        elif versioning_mode == VersioningMode.SAVE_MEM:
            versioning_mode_template1 = open(versioning_mode_dir1 + "/version_all_rows_save_mem.txt", "r").read()
            versioning_mode_template2 = \
                open(versioning_mode_dir2 + "/versioning_query_extensions_save_mem.txt", "r").read()
            update_statement = versioning_mode_template1.format(final_prefixes)
            message = "All rows have been annotated with an artificial end date."
        else:
            raise NoVersioningMode("Versioning mode is neither Q_PERF nor SAVE_MEM. Initial versioning will not be"
                                   "executed. Check also whether an initial timestamp was passed in case of Q_PERF.")

        with open(self._template_location + "/../rdf_star_store/version_all_rows.txt", "w") as vers:
            vers.write(versioning_mode_template1)
        with open(self._template_location + "/../query_utils/versioning_query_extensions.txt", "w") as vers:
            vers.write(versioning_mode_template2)

        self.sparql_post.setQuery(update_statement)
        self.sparql_post.query()

        logging.info(message)

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
        logging.info("Get data ...")
        if yn_timestamp_query:
            if timestamp is None:
                query_utils = QueryUtils(query=select_statement)
            else:
                query_utils = QueryUtils(query=select_statement, citation_timestamp=timestamp)

            query = query_utils.timestamped_query
            logging.info("Timestamped query with timestamp {0} being executed:"
                         " \n {1}".format(query_utils.citation_timestamp, query))
            self.sparql_get_with_post.setQuery(query)
        else:
            logging.info("Query being executed: \n {0}".format(select_statement))
            self.sparql_get_with_post.setQuery(select_statement)
        result = self.sparql_get_with_post.query()
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

        logging.info("{0} rows updated".format(result))

    def insert_triple(self, triples: list, prefixes: dict = None):
        """
        Inserts a list of triples (must be in n3 syntax!) into the RDF* store and two additional (nested) triples
        for each new triple labeling the newly inserted triple with a valid_from and valid_until date.

        :param triples: A list with three elements - a subject, predicate and object. The triple elements must be
        provided in n3 syntax!
        Or: A list of lists with three elements.
        :param prefixes: Prefixes that are used within :param triples
        :return:
        """

        statement = open(self._template_location + "/insert_triple.txt", "r").read()

        if prefixes:
            sparql_prefixes = citation_prefixes(prefixes)
        else:
            sparql_prefixes = citation_prefixes("")

        # Handling input format
        trpls = []
        if not isinstance(triples[0], list) and len(triples) == 3:
            triple = triples
            trpls.append(triple)
        else:
            trpls = triples

        for triple in trpls:
            if isinstance(triple, list) and len(triple) == 3:
                s = triple[0]
                p = triple[1]
                o = triple[2]

                insert_statement = statement.format(sparql_prefixes, s, p, o)
                self.sparql_post.setQuery(insert_statement)
                self.sparql_post.query()
                logging.info("Triple {0} successfully inserted: ".format(triple))
            else:
                e = "Please provide either a list of lists with three elements - subject, predicate and object or a " \
                    "single list with aforementioned three elements in n3 syntax. "
                logging.error(e)
                raise WrongInputFormatException(e)

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
            # Triple statements, filter, ...
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

        logging.info("{0} rows outdated".format(result))

    def _delete_triples(self, triples: list, prefixes: dict = None):
        """
        Deletes the triples and its version annotations from the history. Should be used with care
        as it is most of times not intended to delete triples but to outdate them. This way they will
        still appear in the history and will not appear when querying more recent versions.

        :param triples: Triples in n3 syntax to be deleted
        :param prefixes: Prefixes used in triples.
        :return:
        """

        statement = open(self._template_location + "/_delete_triple.txt", "r").read()

        if prefixes:
            sparql_prefixes = citation_prefixes(prefixes)
        else:
            sparql_prefixes = citation_prefixes("")

        # Handling input format
        trpls = []
        if not isinstance(triples[0], list) and len(triples) == 3:
            triple = triples
            trpls.append(triple)
        else:
            trpls = triples

        for triple in trpls:
            if isinstance(triple, list) and len(triple) == 3:
                s = triple[0]
                p = triple[1]
                o = triple[2]

                delete_statement = statement.format(sparql_prefixes, s, p, o)
                self.sparql_post.setQuery(delete_statement)
                self.sparql_post.query()
                logging.info("Triple {0} successfully deleted: ".format(triple))
            else:
                e = "Please provide either a list of lists with three elements - subject, predicate and object or a " \
                    "single list with aforementioned three elements in n3 syntax. "
                logging.error(e)
                raise WrongInputFormatException(e)
