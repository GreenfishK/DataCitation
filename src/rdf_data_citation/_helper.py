import logging
import os
import configparser
import sys
from datetime import datetime


def config() -> configparser.ConfigParser:
    """
    Load configuration from .ini file.

    :return:
    """
    logging.getLogger().setLevel(10)

    cnf = configparser.ConfigParser()
    rdf_data_citation_module_path = os.path.dirname(sys.modules[config.__module__].__file__)
    config_path = rdf_data_citation_module_path + "/../../config.ini"
    cnf.read(config_path)

    return cnf


def template_path(template_rel_path: str):
    return os.path.join(os.path.dirname(__file__), template_rel_path)


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


def escape_apostrophe(string: str) -> str:
    return string.replace("'", "''")


def citation_timestamp_format(citation_timestamp: datetime) -> str:
    """
    This format is taken from the result set of GraphDB's queries.
    :param citation_timestamp:
    :return:
    """
    return citation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" + citation_timestamp.strftime("%z")[3:5]

