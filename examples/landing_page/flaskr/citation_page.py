from datetime import timezone, timedelta, datetime
from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

import src.rdf_data_citation.rdf_star as rdfs
import src.rdf_data_citation.citation_utils as cu

bp = Blueprint('citation_page', __name__, url_prefix='/datacenter_sample_page_1')


@bp.route('/execute_query', methods=['POST', 'GET'])
def execute_query():
    """
    /execute_query does not exist as a resource but shall only serve the purpose to get the data without
    refreshing the page (AJAX request)
    :return:
    """
    if request.method == 'POST':
        # set up endpoints
        rdf_engine = rdfs.TripleStoreEngine('http://192.168.0.241:7200/repositories/DataCitation',  # GET
                                            'http://192.168.0.241:7200/repositories/DataCitation/statements')  # POST

        query_text = request.form['query_text']
        result_set = rdf_engine.get_data(query_text)  # dataframe
        print(result_set.to_html(header='true'))
        return render_template('datacenter_sample_page_1/citation_page.html',
                               dataframe=result_set.to_html(header='true'))
    # background process happening without any refreshing


# Landing page view
@bp.route('/citation_page', methods=('GET', 'POST'))
def cite():

    if request.method == 'POST':
        print("After the user hits the cite button the result set, "
              "query metadata and citation snippet are returned here")
    return render_template('datacenter_sample_page_1/citation_page.html')


