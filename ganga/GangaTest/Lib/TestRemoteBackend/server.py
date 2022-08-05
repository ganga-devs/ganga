import os
import time

from flask import Flask, request, abort

from GangaCore.Core.GangaThread import GangaThread
from GangaCore.Utility.Config.Config import getConfig


app = Flask(__name__)
app.config['ENV'] = 'development'
test_config = getConfig('TestDummyRemote')
port = test_config['SERVER_PORT']


@app.route("/statusfile", methods=["GET"])
def retrieve_statusfile():
    file_path = request.args.get("path")
    job_id = request.args.get("jid")
    print(f'DummyRemote: Received request from job {job_id} for statusfile at {file_path}')
    time.sleep(getConfig('TestDummyRemote')['SERVER_DEFAULT_DELAY'])
    if not os.path.exists(file_path):
        print(f'DummyRemote: Requested statusfile at {file_path} was not found')
        abort(404)
    with open(file_path, "r") as statusfile:
        stat = statusfile.read()

    return {"stat": stat}


@app.route("/outputfile", methods=["GET"])
def retrieve_outputfile():
    job_id = request.args.get("jid")
    print(f'DummyRemote: Received request from job {job_id} for dummy outpufile')
    time.sleep(getConfig('TestDummyRemote')['FINALISATION_DELAY'])
    return "OK"


class DummyServer(GangaThread):
    def __init__(self):
        GangaThread.__init__(self, name="DummyServer")
        self.daemon = True

    def run(self):
        app.run(port=port, debug=True, use_reloader=False)

    def stop(self):
        exit(0)
