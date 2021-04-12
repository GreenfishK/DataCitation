from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for)
import src.rdf_data_citation.rdf_star as rdfs
from src.rdf_data_citation.citation import Citation
from src.rdf_data_citation.citation_utils import CitationData

# Example citation data and result set description
citation_data = CitationData(identifier="DOI_to_landing_page", creator="Filip Kovacevic",
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
    """
    /execute_query does not exist as a resource but shall only serve the purpose to get the data without
    refreshing the page (AJAX request)
    :return:
    """
    # set up endpoints
    rdf_engine = rdfs.TripleStoreEngine('http://192.168.0.241:7200/repositories/DataCitation',  # GET
                                        'http://192.168.0.241:7200/repositories/DataCitation/statements')  # POST

    query_text = request.form['query_text']
    result_set = rdf_engine.get_data(query_text)  # dataframe
    html_response = render_template('datacenter_sample_page_1/citation_page.html',
                                    dataframe=result_set.to_html(header='true'))
    return html_response


@bp.route('/cite_query', methods=['POST'])
def cite_query():
    """
    Cites the query and returns the citation snippet
    :return:
    """

    citation = Citation('http://192.168.0.241:7200/repositories/DataCitation',  # GET
                        'http://192.168.0.241:7200/repositories/DataCitation/statements')

    query_text = request.form['query_text']
    citation_snippet = citation.cite(query_text, citation_data=citation_data,
                                     result_set_description=result_set_description)

    html_response = render_template('datacenter_sample_page_1/citation_page.html',
                                    citation_snippet=citation_snippet)
    print(html_response)
    return html_response


