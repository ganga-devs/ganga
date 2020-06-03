"""Contains the configuration settings for the GUI Flask App"""

import os

basedir = os.path.abspath(os.path.dirname(__file__))


# TODO Look into ways to generate secret_key without much input from user side - .gangarc?
class Config(object):
    SECRET_KEY = os.environ.get("SECRET_KEY") or "f3aa26t8b537abf6ee6305eefea0a10a"
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(basedir, "gui_db.sqlite")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
