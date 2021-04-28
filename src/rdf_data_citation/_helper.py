import os
import configparser


def config() -> configparser.ConfigParser:
    """
    Load configuration from .ini file.

    :return:
    """
    config = configparser.ConfigParser()
    config.read('../../config.ini')
    return config


def _template_path(template_rel_path: str):
    return os.path.join(os.path.dirname(__file__), template_rel_path)