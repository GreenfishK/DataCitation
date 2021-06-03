import src.rdf_data_citation.rdf_star as rdfs
from src.rdf_data_citation.citation import Citation
from src.rdf_data_citation.exceptions import MissingSortVariables, NoUniqueSortIndexError, ExpressionNotCoveredException
from src.rdf_data_citation.citation_utils import MetaData
import logging
from datetime import datetime, timedelta, timezone
from flask import (Blueprint, flash, g, redirect, Markup, render_template, request, session, url_for)
import configparser

# Load configuration from .ini file.
config = configparser.ConfigParser()
config.read('../../../config.ini')
logging.getLogger().setLevel(int(config.get('TEST', 'log_level')))

# Example citation data and result set description
citation_metadata = MetaData(identifier="DOI_to_landing_page", creator="Filip Kovacevic",
                             title="Judy Chu occurences", publisher="Filip Kovacevic",
                             publication_year="2021", resource_type="Dataset/RDF data",
                             other_citation_data={"Contributor": "Tomasz Miksa"})

result_set_description = "Result set description: All documents where Judy Chu was mentioned with every mention listed"

bp = Blueprint('citation_page', __name__, url_prefix='/datacenter_sample_page_1')


@bp.route('/citation_page', methods=['GET'])
def show_citation_page():
    """
    Just for loading the citation page.
    :return:
    """
    return render_template('datacenter_sample_page_1/citation_page.html')


@bp.route('/execute_query', methods=['POST'])
def execute_query():
    """conda
    /execute_query does not exist as a resource but shall only serve the purpose to get the data without
    refreshing the page (AJAX request)
    :return:
    """
    logging.info("Execute Query")

    # set up endpoints
    rdf_engine = rdfs.TripleStoreEngine(config.get('RDFSTORE', 'get'), config.get('RDFSTORE', 'post'))

    if config.get('VERSIONING', 'yn_init_version_all_applied') == 'False':
        vieTZObject = timezone(timedelta(hours=2))
        # initialcond_timestamp = datetime(2020, 9, 1, 12, 11, 21, 941000, vieTZObject)
        rdf_engine.version_all_rows()
        config.set('VERSIONING', 'yn_init_version_all_applied', 'True')
        with open('../../../config.ini', 'w') as configfile:
            config.write(configfile)

    # Query the latest version of data (as of now)
    try:
        result_set = rdf_engine.get_data(request.form['query_text'])  # dataframe
        number_of_rows = len(result_set.index)
        html_response = render_template('datacenter_sample_page_1/citation_page.html',
                                        dataframe=result_set.to_html(header='true'),
                                        number_of_rows=number_of_rows)
    except ExpressionNotCoveredException as e:
        message = Markup("{0}".format(e))
        flash(message)
        html_response = render_template('datacenter_sample_page_1/citation_page.html',
                                        dataframe='',
                                        number_of_rows='?')
    return html_response


@bp.route('/cite_query', methods=['POST'])
def cite_query():
    """
    Cites the query and returns the citation snippet
    :return:
    """
    logging.info("Cite query")

    citation = Citation(config.get('RDFSTORE', 'get'), config.get('RDFSTORE', 'post'))
    query_text = request.form['query_text']
    logging.info(query_text, citation_metadata, result_set_description)
    try:
        citation_data = citation.cite(query_text, citation_metadata=citation_metadata,
                                      result_set_description=result_set_description)
        citation_snippet = citation_data.citation_metadata.citation_snippet

        html_response = render_template('datacenter_sample_page_1/citation_page.html',
                                        citation_snippet=citation_snippet,
                                        execution_timestamp=citation_data.execution_timestamp,
                                        yn_query_exists=citation_data.yn_query_exists,
                                        yn_result_set_changed=citation_data.yn_result_set_changed,
                                        yn_unique_sort_index=citation_data.yn_unique_sort_index)
        return html_response

    except (NoUniqueSortIndexError, MissingSortVariables, ExpressionNotCoveredException) as e:
        message = Markup("{0}".format(e))
        flash(message)
        html_response = render_template('datacenter_sample_page_1/citation_page.html',
                                        citation_snippet='?',
                                        execution_timestamp='?',
                                        yn_query_exists='?',
                                        yn_result_set_changed='?',
                                        yn_unique_sort_index='False')

        return html_response





