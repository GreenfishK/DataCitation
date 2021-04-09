import os

from flask import Flask


def create_app(test_config=None):
    """

    :param test_config:
    :return:
    """
    # create and configure the app
    # :param instance_relative_config: Tells the app that configuration
    # files are relative to the instance folder. The instance folder is located outside the flaskr package
    # :param SECRET_KEY: is used by Flask and extensions to keep data safe. It’s set to 'dev' to provide
    # a convenient value during development, but it should be overridden with a random value when deploying.
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # a simple page that says hello
    @app.route('/hello')
    def hello():
        return 'Hello, World!'

    return app
