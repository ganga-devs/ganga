import os
import time

from flask import Flask, request, abort

from GangaCore.Core.GangaThread import GangaThread
from GangaCore.Utility.Config.Config import getConfig


app = Flask(__name__)
app.config['ENV'] = 'development'
test_config = getConfig('TestDummyRemote')
port = test_config['SERVER_PORT']
delay_amount = test_config['SERVER_DEFAULT_DELAY']


@app.route("/statusfile", methods=["GET"])
def retrieve_statusfile():
    file_path = request.args.get("path")
    print(f'DummyRemote: Received request for statusfile at {file_path}')
    time.sleep(delay_amount)
    if not os.path.exists(file_path):
        print(f'DummyRemote: Requested statusfile at {file_path} was not found.')
        abort(404)
    with open(file_path, "r") as statusfile:
        stat = statusfile.read()

    return {"stat": stat}


class DummyServer(GangaThread):
    def __init__(self):
        GangaThread.__init__(self, name="DummyServer")
        self.daemon = True

    def run(self):
        app.run(port=port, debug=True, use_reloader=False)

    def stop(self):
        exit(0)
