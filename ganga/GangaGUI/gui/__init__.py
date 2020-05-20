from flask import Flask
from GangaGUI.config import Config
import sys

# Disable development server warning (as it is not applicable to our GUI Flask App)
cli = sys.modules['flask.cli']
cli.show_server_banner = lambda *x: None

# Flask App and get configuration from config.py file
app = Flask(__name__)
app.config.from_object(Config)

from GangaGUI.gui import routes
