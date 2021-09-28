import os
import string
import uuid
import random
import requests
import subprocess
import socket
from contextlib import closing
from GangaCore.Core.GangaThread import GangaThread
from GangaCore.Utility.logging import getLogger
from GangaGUI.gui.routes import gui, db
from GangaGUI import guistate
from GangaGUI.api import internal
from GangaGUI.gui.routes import User

# Ganga logger
logger = getLogger()

# Directory just level up from GangaGUI
ganga_package_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# GangaThread for Internal API Flask server
class APIServerThread(GangaThread):

    def __init__(self, name: str, host: str, port: int):
        super().__init__(name=name)
        self.host = host
        self.port = port

    def run(self):
        try:
            internal.run(host=self.host, port=self.port)
        except Exception as err:
            logger.error(f"Failed to start API Server: {str(err)}")

    def shutdown(self):
        res = requests.post("http://localhost:{port}/shutdown".format(port=self.port))
        logger.info(f"{res.status_code} - Success {res.json()['success']}, {res.json()['message']}")
        if res.status_code == 200:
            return True
        return False
    
    
def free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def start_gui(*, gui_host: str = "127.0.0.1", gui_port: int = free_port(), internal_port: int = free_port(),
              password: str = None, only_internal: bool = False):
    """
    Start GUI Flask App on a Gunicorn server and API Flask App on a GangaThread

    :param only_internal: bool - will only start the internal API server
    :param gui_host: str
    :param gui_port: int
    :param internal_port: int
    :param password: str

    Returns (host, port, user, password)
    Accepts "gui_host", "gui_port", "internal_port" and "password" as arguments.

    By default the "gui_host" is set to "127.0.0.1". It means GUI will be accessible over the network.
    In order to make to accessible only inside the local machine, please set the host to "localhost" as in start_gui(gui_host="localhost").

    "gui_port" can be set to any free port available (default is 5500)
    If GUI is to be accessed over the network make sure the firewall allows the specified port.

    "internal_port" can be set to any free port available (default is 5000)
    "internal_port" is used by the API flask server which run on a Ganga Thread. This server has access to all of the
    Ganga functions and information. The GUI Server communicates with the API Server over a RESTful interface for querying the data from Ganga.
    The API server is not accessible from outside the machine, it is only meant to be accessed by the GUI Server.
    The API server is a weak server and can not be use to deliver GUI over the internet.

    Use the "user" and "password" to log into the GUI or generate token to access the APIs

    The default "user" is "GangaGUIAdmin", and will be created during the first run of the GangaGUI.
    "GangaGUIAdmin" has the admin rights with respect to GangaGUI ONLY.

    If the "password" is not specified, a random 7 character AlphaNumeric password is auto generated.

    Example Usage:
    start_gui() -> will return ("127.0.0.1", 5500, "GangaGUIAdmin", "RNDPASS")
    start_gui(password="mypassword") -> will return ("127.0.0.1", 5500, "GangaGUIAdmin", "mypassword")
    start_gui(host="127.0.0.1", password="mypassword") -> will return ("127.0.0.1", 5500, "GangaGUIAdmin", "mypassword")
    start_gui(host="127.0.0.1", port=1234, password="mypassword") -> ("127.0.0.1", 1234, "GangaGUIAdmin", "mypassword")
    """

    # For when it is called by ganga-gui binary for starting the integrated terminal
    if os.environ.get('WEB_CLI') is not None or only_internal is not False:

        # Get the internal port to start the API server on
        if os.environ.get("INTERNAL_PORT") is not None:
            internal_port = int(os.environ.get("INTERNAL_PORT"))

        # Start internal API server, it is always be meant for internal use by the GUI server
        api_server = APIServerThread("GangaGUIAPI", "localhost", internal_port)
        guistate.set_api_server(api_server)
        api_server.start()

        return True

    # Create default user
    gui_user, gui_password = create_default_user(password=password)

    # Start internal API server, it is always be meant for internal use by the GUI server
    api_server = APIServerThread("GangaGUIAPI", "localhost", internal_port)
    guistate.set_api_server(api_server)
    api_server.start()

    # Start GUI server
    gui_server = start_gui_server(gui_host=gui_host, gui_port=gui_port, internal_port=internal_port,
                                  package_dir=ganga_package_dir)
    guistate.set_gui_server(gui_server)

    # Display necessary information to the user
    logger.info(f"GUI Login Details: user='{gui_user.user}', password='{gui_password}'")
    logger.info(f"You can now access the GUI at http://{gui_host}:{gui_port}")
    logger.info(f"The internal port for flask server using ganga thread is http://{gui_host}:{internal_port}")
    logger.info(
        f"If on a remote system you may need to set up port forwarding to reach the web server. This can be done with 'ssh -D {gui_port} <remote-ip>' from a terminal.")

    return gui_host, gui_port, gui_user.user, gui_password


def stop_gui():
    """Stop API Flask server on a GangaThread and the GUI Flask server running on a Gunicorn server"""

    if guistate.get_api_server() is not None:
        if guistate.get_api_server().shutdown():
            guistate.set_api_server(None)
        else:
            raise Exception("Error in shutting down the API server.")

    if guistate.get_gui_server() is not None:
        guistate.get_gui_server().terminate()


# ******************** Helper Functions ******************** #


def create_default_user(password=None):
    """
    Creates default user called GangaGUIAdmin.
    :param password: str
    :return: tuple
    """

    # Database path
    db_path = gui.config["SQLALCHEMY_DATABASE_URI"].replace("sqlite:///", "")

    if not os.path.exists(db_path):
        # Create database if does not exist
        db.create_all()
        gui_user = User()
        gui_user.public_id = str(uuid.uuid4())
        gui_user.user = "GangaGUIAdmin"
        gui_user.role = "Admin"
        if password is None:
            password = ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))
        gui_user.store_password_hash(password)
        db.session.add(gui_user)
        db.session.commit()
    else:
        # Store password for GangaGUIAdmin in database
        gui_user = User.query.filter_by(user="GangaGUIAdmin").first()
        if gui_user is None:
            gui_user = User()
            gui_user.user = "GangaGUIAdmin"
            gui_user.role = "Admin"
        if password is None:
            password = ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))
        gui_user.store_password_hash(password)
        gui_user.public_id = str(uuid.uuid4())
        db.session.add(gui_user)
        db.session.commit()

    return gui_user, password


def start_gui_server(gui_host, gui_port, internal_port, package_dir=ganga_package_dir, web_cli_mode=False,
                     web_cli_port=None):
    """
    Start the GUI server on a Gunicorn server - this is started as a separate process and communicated with the Internal API server running on a GangaThread which has access to Ganga resources.

    :param gui_host: str
    :param gui_port: str
    :param internal_port: int or str
    :param package_dir: str
    :return: Popen
    """

    # Start the GUI on a Gunicorn (production ready) server.

    # Set the env for the GUI server
    gui_env = os.environ.copy()
    gui_env["INTERNAL_PORT"] = str(internal_port)
    # If started by ganga-gui binary to start the web cli
    if web_cli_mode:
        if web_cli_port is None:
            raise Exception("Please provide Web CLI Port.")
        gui_env["WEB_CLI"] = str(True)
        gui_env["WEB_CLI_PORT"] = str(int(web_cli_port))

    # Log location
    gui_accesslog_file = gui.config["ACCESS_LOG"]
    gui_errorlog_file = gui.config["ERROR_LOG"]

    # Start the server
    gui_server = subprocess.Popen(
        f"gunicorn --chdir {package_dir} --log-level warning --access-logfile {gui_accesslog_file} --error-logfile {gui_errorlog_file} --bind {gui_host}:{gui_port} wsgi:gui",
        shell=True, cwd=os.path.dirname(os.path.abspath(__file__)), env=gui_env)

    return gui_server


# ******************** EOF ******************** #

# TODO Remove
# Use this for starting the server for development purposes
# Development user="GangaGUIAdmin" password="GangaGUIAdmin"
if __name__ == "__main__":
    # TODO fix password
    db.create_all()
    user = User(public_id=str(uuid.uuid4()), user="GangaGUIAdmin", role="Admin")
    user.store_password_hash("GangaGUIAdmin")
    db.session.add(user)
    db.session.commit()
    gui.logger.warning("Development server running with debugger active. user='GangaGUIAdmin' password='GangaGUIAdmin'")
    gui.run(port=5500, debug=True)
