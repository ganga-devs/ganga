from flask import Flask
from GangaGUI.config import Config
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
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

# Login manage for the view routes
login = LoginManager(app)
login.login_view = "login"
login.login_message = "Please Login to Access this Page."
login.login_message_category = "warning"

from GangaGUI.gui import routes
