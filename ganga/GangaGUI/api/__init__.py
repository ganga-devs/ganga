from flask import Flask
import sys
import os
import logging

# Disable development server warning (as it is not applicable to our GUI Flask App)
cli = sys.modules['flask.cli']
cli.show_server_banner = lambda *x: None

# Only error cases are logged
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Flask app for APIs
internal = Flask(__name__)
internal.config['SECRET_KEY'] = os.urandom(24)

from GangaGUI.api import routes
