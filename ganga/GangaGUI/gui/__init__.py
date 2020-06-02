from flask import Flask
from GangaGUI.config import Config
from flask_sqlalchemy import SQLAlchemy
import sys

# Disable development server warning (as it is not applicable to our GUI Flask App)
cli = sys.modules['flask.cli']
cli.show_server_banner = lambda *x: None

# Flask App and get configuration from config.py file
app = Flask(__name__)
app.config.from_object(Config)

# Database object which is used to interact with the "gui_db.sqlite"
# NOTE: IT HAS NO RELATION WITH THE GANGA PERSISTENT DATABASE
db = SQLAlchemy(app)

from GangaGUI.gui import routes
