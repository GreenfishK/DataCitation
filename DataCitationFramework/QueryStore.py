import sqlalchemy as sql
from sqlalchemy import exc
from DataCitationFramework.QueryUtils import Query
import pandas as pd
import datetime


def escape_apostrophe(string: str) -> str:
    return string.replace("'", "''")


class QueryStore:

    def __init__(self, relative_path_to_db):
        """
        Tables: citation_hub
        :param relative_path_to_db:
        """
        self.engine = sql.create_engine("sqlite:///{0}".format(relative_path_to_db))

    def _remove(self, query_checksum):
        delete_query_citation = "Delete from query_citation where query_checksum = :query_checksum "
        delete_query = "Delete from query_hub where query_checksum = :query_checksum "

        with self.engine.connect() as connection:
            try:
                # Order is important due to referential integrity
                connection.execute(delete_query_citation, query_checksum=query_checksum)
                connection.execute(delete_query, query_checksum=query_checksum)
                print("Query with checksum {0} removed from query store".format(query_checksum))

            except Exception as e:
                print(e)

    def lookup(self, query_checksum: str) -> Query:
        """

        :param query_checksum:
        :return:
        """
        # TODO: Add result set description to query_citation?
        # TODO: Store citation timestamp? It would be include redundant information
        select_statement = "select b.query_pid, a.query_checksum, a.orig_query, a.normal_query, " \
                           "b.result_set_checksum, b.citation_timestamp, b.citation_snippet " \
                           "from query_hub a join query_citation b " \
                           "on a.query_checksum = b.query_checksum " \
                           "where a.query_checksum = :query_checksum and citation_timestamp =" \
                           "(select max(citation_timestamp) from query_citation c " \
                           "where a.query_checksum = c.query_checksum)"
        with self.engine.connect() as connection:
            try:
                result = connection.execute(select_statement, query_checksum=query_checksum)
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()
                query = Query(df.orig_query)
                query.normalized_query_algebra = df.normal_query.loc[0]
                query.result_set_checksum = df.result_set_checksum.loc[0]
                query_datetime = df.citation_timestamp.loc[0]
                citation_timestamp = datetime.datetime.strptime(query_datetime, "%Y-%m-%dT%H:%M:%S.%f%z")
                query.citation_timestamp = citation_timestamp
                query.query_pid = df.query_pid.loc[0]

                return query
            except Exception as e:
                print(e)

    def store(self, query: Query, citation_snippet: str, yn_new_query=True):
        """

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

        :param query:
        :param citation_snippet:
        :param yn_new_query: True, if the query is not found by its checksum in the query table. Otherwise false.
        :return:
        """

        insert_statement = "insert into query_hub(query_checksum, orig_query, normal_query) " \
                           "values (:query_checksum, :orig_query, :normal_query)"

        insert_statement_2 = "insert into query_citation(query_pid, query_checksum, result_set_checksum," \
                             "citation_snippet, citation_timestamp) " \
                             "values (:query_pid, :query_checksum, :result_set_checksum," \
                             " :citation_snippet, :citation_timestamp)"

        with self.engine.connect() as connection:
            if yn_new_query:
                try:
                    connection.execute(insert_statement,
                                       query_checksum=query.query_checksum,
                                       orig_query=query.query,
                                       normal_query=query.normalized_query_algebra)
                except exc.IntegrityError as e:
                    print("A query is trying to be inserted that exists already. The checksum of the executed query "
                          "is found within the query_hub table")
            try:
                connection.execute(insert_statement_2,
                                   query_pid=query.query_pid,
                                   query_checksum=query.query_checksum,
                                   result_set_checksum=query.result_set_checksum,
                                   citation_snippet=citation_snippet,
                                   citation_timestamp=query.citation_timestamp)
            except exc.IntegrityError as e:
                print("A query is trying to be inserted that exists already. The query PID of the executed query "
                      "is found within the query_citation table")

            print("Query with checksum {0} and query_pid {1} stored".format(query.query_checksum, query.query_pid))

    def citation_snippet(self, query_pid: str):
        """

        :param query_pid:
        :return:
        """
        select_statement = "Select citation_snippet from query_citation where query_pid = :query_pid"
        with self.engine.connect() as connection:
            try:
                result = connection.execute(select_statement, query_pid=query_pid)
                df = pd.DataFrame(result.fetchall())
                df.columns = result.keys()
                snippet = df.citation_snippet.loc[0]
            except Exception as e:
                print(e)
        return snippet
