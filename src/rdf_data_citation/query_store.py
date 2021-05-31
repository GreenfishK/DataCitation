import logging

from src.rdf_data_citation.citation_utils import QueryUtils, RDFDataSetUtils, MetaData
from src.rdf_data_citation._helper import template_path
from src.rdf_data_citation.exceptions import QueryExistsError
import sqlalchemy as sql
from sqlalchemy import exc
import pandas as pd


class QueryStore:

    def __init__(self):
        """

        Tables: query_hub, query_citation
        """

        self.path_to_persistence = template_path("persistence")
        db_path = self.path_to_persistence + "/query_store.db"
        self.engine = sql.create_engine("sqlite:///{0}".format(db_path))

    def _remove(self, query_checksum):
        delete_query_citation = "Delete from query_citation where query_checksum = :query_checksum "
        delete_query = "Delete from query_hub where query_checksum = :query_checksum "

        with self.engine.connect() as connection:
            try:
                # Order is important due to referential integrity
                connection.execute(delete_query_citation, query_checksum=query_checksum)
                connection.execute(delete_query, query_checksum=query_checksum)
                print("QueryData with checksum {0} removed from query store".format(query_checksum))

            except Exception as e:
                print(e)

    def retrieve(self, query_pid: str):
        """
        Retrieves the full query object which shall be displayed on the landing page.

        :param query_pid:
        :return:
        """
        pass

    def lookup(self, query_checksum: str) -> [QueryUtils, RDFDataSetUtils, MetaData]:
        """
        Checks whether the query exists in the database. If yes, query data, dataset metadata and citation metadata
        (including provenance data and the citation snippet) are returned.

        :param query_checksum:
        :return:
        """

        select_statement = open("{0}/lookup_select.sql".format(self.path_to_persistence), "r").read()
        with self.engine.connect() as connection:
            try:
                result = connection.execute(select_statement, query_checksum=query_checksum)
                df = pd.DataFrame(result.fetchall())

                if df.empty:
                    return [None, None, None]

                df.columns = result.keys()
                query_data = QueryUtils()
                query_data.checksum = df.query_checksum.loc[0]
                query_data.pid = df.query_pid.loc[0]
                query_data.normal_query_algebra = df.normal_query_algebra.loc[0]
                query_data.normal_query_algebra = df.normal_query.loc[0]
                query_data.sparql_prefixes = df.query_prefixes.loc[0]
                query_data.citation_timestamp = df.citation_timestamp.loc[0]

                result_set_data = RDFDataSetUtils()
                result_set_data.dataset = df
                result_set_data.checksum = df.result_set_checksum.loc[0]
                result_set_data.description = df.result_set_description.loc[0]
                result_set_data.sort_order = df.result_set_sort_order.loc[0]

                meta_data = MetaData()
                meta_data.set_metadata(df.citation_data.loc[0])
                meta_data.citation_snippet = df.citation_snippet.loc[0]

                print("New query checksum: {0}; Existing query checksum: {1}".format(
                    query_checksum, df.query_checksum.loc[0]))

                return [query_data, result_set_data, meta_data]
            except Exception as e:
                print(e)

    def store(self, query_data: QueryUtils, rs_data: RDFDataSetUtils, meta_data: MetaData, yn_new_query=True):
        """
        R9 - Store Query
        common metadata:
        author*
        publication date*
        title*
        edition*
        version
        URI: query checksum
        resource type
        publisher
        unique number fingerprint: result set checksum
        persistent URI: query checksum
        location

        reference: Theory and Practice of Data Citation, Silvello et al.

        :param query_data:
        :param rs_data:
        :param meta_data: A set of mandatory attributes from DataCite's Metadata Schema and additional
        attributes that  might be provided
        :param yn_new_query: True, if the query is not found by its checksum in the query table. Otherwise false.
        :return:
        """

        insert_statement = open("{0}/store_insert_query_hub.sql".format(self.path_to_persistence), "r").read()
        insert_statement_2 = open("{0}/store_insert_query_citation.sql".format(self.path_to_persistence), "r").read()
        update_statement = open("{0}/store_update_query_hub.sql".format(self.path_to_persistence), "r").read()

        with self.engine.connect() as connection:
            if yn_new_query:
                try:
                    connection.execute(insert_statement,
                                       query_checksum=query_data.checksum,
                                       orig_query=query_data.query,
                                       query_prefixes=query_data.sparql_prefixes,
                                       normal_query_algebra=str(query_data.normal_query_algebra.algebra),
                                       normal_query=query_data.normal_query)
                    print("New query with checksum {0} and PID {1} stored".format(query_data.checksum,
                                                                                  query_data.pid))
                except exc.IntegrityError:
                    raise QueryExistsError("A query is trying to be inserted that exists already. "
                                           "The checksum of the executed query "
                                           "is found within the query_hub table: {0}".format(query_data.checksum))
            try:
                print(rs_data.sort_order)
                connection.execute(insert_statement_2,
                                   query_pid=query_data.pid,
                                   query_checksum=query_data.checksum,
                                   result_set_checksum=rs_data.checksum,
                                   result_set_description=rs_data.description,
                                   result_set_sort_order=", ".join(rs_data.sort_order),
                                   citation_data=meta_data.to_json(),
                                   citation_snippet=meta_data.citation_snippet,
                                   citation_timestamp=query_data.citation_timestamp)
                if not yn_new_query:
                    print("A new citation has been added to the existing query with checksum {0} ."
                          "The new entry carries the PID {1}".format(query_data.checksum, query_data.pid))

            except exc.IntegrityError:
                raise QueryExistsError("A query is trying to be inserted that already exists. The query PID {0} "
                                       "of the executed query is found within the "
                                       "query_citation table".format(query_data.pid))

            try:
                connection.execute(update_statement, query_checksum=query_data.checksum)
            except Exception:
                print("Could not update the last citation PID in query_hub.last_citation_pid")
