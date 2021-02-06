import sqlite3
import sqlalchemy
from QueryUtils import Query
import datetime


def lookup(query_checksum: str) -> Query:
    """

    :param query_checksum:
    :return:
    """
    pass


def append_citation_date(query: Query, citation_timestamp: datetime):
    """

    :param query:
    :param citation_timestamp:
    :return:
    """
    pass


def store(query: Query, citation_snippet: str, result_set_description: str):
    """

    :param query:
    :param citation_snippet:
    :param result_set_description:
    :return:
    """
    pass


def citation_snippet(query_pid: str):
    """

    :param query_pid:
    :return:
    """
    snippet = ""

    return snippet
