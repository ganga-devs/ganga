from GangaCore.Core.GangaThread import GangaThread
from GangaCore.Utility.logging import getLogger
from GangaGUI.gui import app
import requests

logger = getLogger()
gui_server = None


# GangaThread for Flask Server
class GUIServerThread(GangaThread):

    def __init__(self, name: str, host, port):
        GangaThread.__init__(self, name=name)
        self.host = host
        self.port = port

    def run(self):
        try:
            logger.info("Starting GUI Server at {}:{}".format(self.host, self.port))
            app.run(host=self.host, port=self.port)
        except Exception as err:
            logger.error("Failed to start GUI Server: {}".format(err))

    def shutdown(self):
        res = requests.post("http://localhost:{port}/shutdown".format(port=self.port))
        logger.info("{} - {}".format(res.status_code, res.text))


def start_gui(host: str = "localhost", port: int = 5000):
    """Start GUI Flask App on a GangaThread"""
    global gui_server
    gui_server = GUIServerThread("GangaGUI", host, port)
    gui_server.start()


def stop_gui():
    """Stop GUI Flask App on a GangaThread"""
    global gui_server
    if gui_server is not None:
        gui_server.shutdown()


# Use this for starting the server for development purposes
if __name__ == "__main__":
    app.run(debug=True)
