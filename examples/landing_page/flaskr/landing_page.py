import functools
from db import get_db

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

bp = Blueprint('landing_page', __name__, url_prefix='/datacenter_sample_page_1')


@bp.route('/landing_page', methods=('GET', 'POST'))
def resolve():
    """
    After the user calls the PID URL he gets directed to the landing page

    :return:
    """
    return render_template('datacenter_sample_page_1/landing_page.html')

