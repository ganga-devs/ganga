"""Contains the configuration settings for the GUI Flask App"""

import os

from GangaCore import getConfig

# Directories
gangadir = getConfig("Configuration")["gangadir"]
ganga_logfile = getConfig("Logging")["_logfile"]
gangarc_file = getConfig("System")["GANGA_CONFIG_FILE"]
gui_dir = os.path.join(gangadir, "gui")

# Make GUI folder if doesn't exist
if not os.path.exists(gui_dir):
    os.makedirs(gui_dir)

# Make GUI essential folders
for dir_name in ["upload", "logs", "storage"]:
    dir_path = os.path.join(gui_dir, dir_name)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


class Config(object):
    SECRET_KEY = os.urandom(24)

    # Database related configuration
    SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(gui_dir, "gui.sqlite")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # GUI specific config
    UPLOAD_FOLDER = os.path.join(gui_dir, "upload")
    GUI_FOLDER = gui_dir
    LOGS_FOLDER = os.path.join(gui_dir, "logs")
    STORAGE_FOLDER = os.path.join(gui_dir, "storage")
    

    # Logs config
    ACCESS_LOG = os.path.join(LOGS_FOLDER, "gui_access.log")
    ERROR_LOG = os.path.join(LOGS_FOLDER, "gui_error.log")
    GANGA_RC = gangarc_file
    GANGA_LOG = os.path.expanduser(ganga_logfile)
    

    # Web CLI related config
    WEB_CLI = False
    GANGA_ARGS = ""
    INTERNAL_PORT = None

    # To store pseudo terminal file descriptor (connected to the child’s controlling terminal) and child pid
    FD = None
    CHILD_PID = None
