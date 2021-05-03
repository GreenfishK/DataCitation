import os
import configparser
from datetime import datetime


def config() -> configparser.ConfigParser:
    """
    Load configuration from .ini file.

    :return:
    """
    config = configparser.ConfigParser()
    config.read('../../config.ini')
    return config


def template_path(template_rel_path: str):
    return os.path.join(os.path.dirname(__file__), template_rel_path)


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


def citation_timestamp_format(citation_timestamp: datetime) -> str:
    """
    This format is taken from the result set of GraphDB's queries.
    :param citation_timestamp:
    :return:
    """
    return citation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" + citation_timestamp.strftime("%z")[3:5]


def prefixes_to_sparql(prefixes: dict) -> str:
    """
    Converts a dict of prefixes to a string with SPARQL syntax for prefixes.
    :param prefixes:
    :return: SPARQL prefixes as string
    """
    if prefixes is None:
        return ""

    sparql_prefixes = ""
    for key, value in prefixes.items():
        sparql_prefixes += "PREFIX {0}: <{1}> \n".format(key, value)
    return sparql_prefixes


def attach_prefixes(query, prefixes: dict) -> str:
    """
    Attaches prefixes in SPARQL syntax to the SPARQL query. The passed query should therefore have no prefixes.

    :param query:
    :param prefixes:
    :return:
    """
    template = open(template_path("templates/query_utils/prefixes_query_wrapper.txt"), "r").read()
    sparql_prefixes = prefixes_to_sparql(prefixes)
    query_with_prefixes = template.format(sparql_prefixes, query)
    return query_with_prefixes