import os
import string
import uuid
import random
import requests
from GangaCore.Core.GangaThread import GangaThread
from GangaCore.Utility.logging import getLogger
from GangaGUI.gui import app, db
from GangaGUI.gui.models import User


logger = getLogger()
gui_server = None


# GangaThread for Flask Server
class GUIServerThread(GangaThread):

    def __init__(self, name: str, host: str, port: int):
        GangaThread.__init__(self, name=name)
        self.host = host
        self.port = port

    def run(self):
        try:
            logger.info("You can now access the GUI at http://{}:{}".format(self.host, self.port))
            logger.info("If on a remote system you may need to set up port forwarding to reach the web server. This "
                        "can be done with 'ssh -D {} <remote-ip>' from a terminal.".format(self.port)) 
            app.run(host=self.host, port=self.port)
        except Exception as err:
            logger.error("Failed to start GUI Server: {}".format(err))

    def shutdown(self):
        res = requests.post("http://localhost:{port}/shutdown".format(port=self.port))
        logger.info("{} - Success {}, {}".format(res.status_code, res.json()["success"], res.json()["message"]))
        if res.status_code == 200:
            return True
        return False
        

def start_gui(host: str = "localhost", port: int = 5000, password: str = None) -> tuple:
    """
    Start GUI Flask App on a GangaThread

    :param host: str
    :param port: int
    :param password: str
    :return: tuple

    Returns (host, port, user, password)
    Accepts "host", "port" and "password" as arguments.

    By default the "host" is set to "localhost". It means GUI will NOT be accessible over the network.
    In order to make to accessible over the network, please set the host to "0.0.0.0" as in start_gui(host="0.0.0.0").

    "port" can be set to any free port available (default is 5000)
    If GUI is to be accessed over the network make sure the firewall allows the specified port.

    Use the "user" and "password" to log into the GUI or generate token to access the APIs

    The default "user" is "GangaGUIAdmin", and will be created during the first run of the GangaGUI.
    "GangaGUIAdmin" has the admin rights with respect to GangaGUI ONLY.

    If the "password" is not specified, a random 7 character AlphaNumeric password is auto generated.

    Example Usage:
    start_gui() -> will return ("localhost", 5000, "GangaGUIAdmin", "RNDPASS")
    start_gui(password="mypassword") -> will return ("localhost", 5000, "GangaGUIAdmin", "mypassword")
    start_gui(host="0.0.0.0", password="mypassword") -> will return ("0.0.0.0", 5000, "GangaGUIAdmin", "mypassword")
    start_gui(host="0.0.0.0", port=1234, password="mypassword") -> ("0.0.0.0", 1234, "GangaGUIAdmin", "mypassword")
    """

    db_path = app.config["SQLALCHEMY_DATABASE_URI"][10:]

    if not os.path.exists(db_path):
        # Create database if does not exist
        db.create_all()
        user = User(public_id=str(uuid.uuid4()), user="GangaGUIAdmin", role="Admin")
        if password is None:
            password = ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))
        user.store_password_hash(password)
        db.session.add(user)
        db.session.commit()
    else:
        # Store password for GangaGUIAdmin in database
        user = User.query.filter_by(user="GangaGUIAdmin").first()
        if user is None:
            user = User(user="GangaGUIAdmin", role="Admin")
        if password is None:
            password = ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))
        user.store_password_hash(password)
        user.public_id = str(uuid.uuid4())
        db.session.add(user)
        db.session.commit()

    # Start server
    global gui_server
    gui_server = GUIServerThread("GangaGUI", host, port)
    logger.info("GUI Login Details: user='{}', password='{}'".format(user.user, password))
    gui_server.start()
    return host, port, user.user, password


def stop_gui():
    """Stop GUI Flask App on a GangaThread"""
    global gui_server
    if gui_server is not None:
        if gui_server.shutdown():
            gui_server = None
        else:
            raise Exception("Error in shutting down the GUI server.")


# Use this for starting the server for development purposes
# Development user="GangaGUIAdmin" password="GangaGUIAdmin"
if __name__ == "__main__":
    db.create_all()
    user = User(public_id=str(uuid.uuid4()), user="GangaGUIAdmin", role="Admin")
    user.store_password_hash("GangaGUIAdmin")
    db.session.add(user)
    db.session.commit()
    app.logger.warning("Development server running with debugger active. user='GangaGUIAdmin' password='GangaGUIAdmin'")
    app.run(debug=True)
