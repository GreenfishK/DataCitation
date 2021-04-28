import os
from flask import Flask, url_for
import db
from src.rdf_data_citation import persistence
import landing_page
import citation_page


def create_app(test_config=None):
    """
    citation page is on http://127.0.0.1:5000/datacenter_sample_page_1/citation_page
    :param test_config:
    :return:
    """

    app = Flask(__name__, instance_relative_config=True)
    app.secret_key = os.urandom(24)
    app.register_blueprint(citation_page.bp)
    app.register_blueprint(landing_page.bp)

    return app


create_app().run()
