import sqlite3
import sqlalchemy as sql
from DataCitationFramework.QueryUtils import Query
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
        select_statement = "select * from query a join query_citation b " \
                           "on a.query_checksum = b.query_checksum " \
                           "where checksum = {0} and citation_timestamp =" \
                           "(select max(citation_timestamp) from query_citation" \
                           "where a.query_checksum = query_checksum)".format(query_checksum)
        with self.engine.connect() as connection:
            try:
                result = connection.execute(select_statement)
            except Exception as e:
                print(e)

        query = Query(result.orig_query)
        query.citation_timestamp = result.citation_timestamp
        query.query_pid = result.query_PID

        return query

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
