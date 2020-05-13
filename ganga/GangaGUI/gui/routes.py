from GangaGUI.gui import app
from flask import request


@app.route("/")
def dashboard():
    return "Hello GangaGUI"


@app.route('/shutdown', methods=['POST'])
def shutdown():
    func = request.environ.get('werkzeug.server.shutdown')
    func()
    return 'Shutting down GUI...'
