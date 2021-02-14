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
        query = "select * from citation_data where checksum = {0}".format(query_checksum)
        with self.engine.connect() as connection:
            try:
                result = connection.execute(query)
            except Exception as e:
                print(e)
        return result

    def insert_new_query_version(self, query: Query, citation_timestamp: datetime):
        """

        :param query:
        :param citation_timestamp:
        :return:
        """

    def store(self, query: Query, citation_snippet: str):
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

        :param citation_snippet:
        :param citation_data:
        :param query:
        :return:
        """

        insert_statement = "insert into citation_data(query_PID, query_checksum, result_set_checksum, " \
                           "orig_query, normal_query, query_timestamp, execution_timestamp, result_set_desc, " \
                           "citation_data) values (""{0}"", ""{1}"", ""{2}"", ""{3}"", ""{4}""," \
                           " ""{5}"", ""{6}"", ""{7}"")".format(query.query_pid,
                                                                query.query_checksum,
                                                                query.result_set_checksum,
                                                                query.query,
                                                                query.normalized_query_algebra,
                                                                query.citation_timestamp,
                                                                query.citation_timestamp,
                                                                citation_snippet)
        with self.engine.connect() as connection:
            try:
                connection.execute(insert_statement)
            except Exception as e:
                print(e)

    def citation_snippet(self, query_pid: str):
        """

        :param query_pid:
        :return:
        """
        snippet = ""

        return snippet
