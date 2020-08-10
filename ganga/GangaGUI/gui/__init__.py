from flask import Flask
from GangaGUI.gui.config import Config
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

# GUI Flask App and set configuration from config.py file
gui = Flask(__name__)
gui.config.from_object(Config)

# Database object which is used to interact with the "gui.sqlite" in gangadir/gui folder
# NOTE: IT HAS NO RELATION WITH THE GANGA PERSISTENT DATABASE
db = SQLAlchemy(gui)

# Login manage for the view routes
login = LoginManager(gui)
login.login_view = "login"
login.login_message = "Please Login to Access this Page."
login.login_message_category = "warning"

from GangaGUI.gui import routes
