import sqlalchemy as sql
from DataCitationFramework.QueryUtils import Query
import pandas as pd
import datetime
import json


class QueryStore:

    def __init__(self, relative_path_to_db):
        self.engine = sql.create_engine("sqlite:///{0}".format(relative_path_to_db))

    def lookup(self, query_checksum: str) -> Query:
        """

        :param query_checksum:
        :return:
        """
        select_statement = "select b.query_pid, a.query_checksum, a.orig_query, a.normal_query, " \
                           "b.result_set_checksum, b.result_set_desc, b.citation_timestamp, b.citation_snippet " \
                           "from query a join query_citation b " \
                           "on a.query_checksum = b.query_checksum " \
                           "where a.query_checksum = {0} and citation_timestamp =" \
                           "(select max(citation_timestamp) from query_citation c " \
                           "where a.query_checksum = c.query_checksum)".format(query_checksum)
        with self.engine.connect() as connection:
            try:
                result = connection.execute(select_statement)
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

        insert_statement = "insert into citation_data(query_checksum, " \
                           "orig_query, normal_query, citation_timestamp, " \
                           "citation_data) values (""{0}"", ""{1}"", ""{2}"")".format(query.query_checksum,
                                                                                      query.query,
                                                                                      query.normalized_query_algebra)
        insert_statement_2 = "insert into query_executions(query_PID, query_checksum, result_set_checksum," \
                             "result_set_desc, citation_snippet, citation_timestamp) " \
                             "values (""{0}"", ""{1}"", ""{2}"", ""{3}"", ""{4}"")".format(query.query_pid,
                                                                                           query.query_checksum,
                                                                                           query.result_set_checksum,
                                                                                           citation_snippet,
                                                                                           query.citation_timestamp)
        with self.engine.connect() as connection:
            try:
                if yn_new_query:
                    connection.execute(insert_statement)
                connection.execute(insert_statement_2)
            except Exception as e:
                print(e)

    def citation_snippet(self, query_pid: str):
        """

        :param query_pid:
        :return:
        """
        snippet = ""

        return snippet
