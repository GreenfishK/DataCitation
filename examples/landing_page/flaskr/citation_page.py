import functools
from db import get_db

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

bp = Blueprint('citation_page', __name__, url_prefix='/datacenter_sample_page_1')


# Landing page view
@bp.route('/citation_page', methods=('GET', 'POST'))
def cite():

    if request.method == 'POST':
        print("After the user hits the cite button the result set, "
              "query metadata and citation snippet are returned here")
    return render_template('datacenter_sample_page_1/citation_page.html')


def show_result_set():
    pass