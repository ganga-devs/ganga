import pty
import os
import subprocess
import select
import termios
import struct
import fcntl
from flask import Flask, render_template, redirect, url_for, jsonify
from flask_socketio import SocketIO

# Flask app for Web CLI
web_cli = Flask(__name__)
web_cli.config["SECRET_KEY"] = "f3aa26t8b537abf6ee6305ddfe2wr10a"  # TODO Generate secret key

# To store pseudo terminal file descriptor (connected to the child’s controlling terminal) and child pid
web_cli.config["FD"] = None
web_cli.config["CHILD_PID"] = None

# For websocket, for communication between frontend and backend
socketio = SocketIO(web_cli)


# ******************** Main Routes ******************** #


# Run before first request - start ganga in a pseudo terminal with --webgui
@web_cli.before_first_request
def first_run():
    # Create child process attached to a pty that we can read from and write to
    (child_pid, fd) = pty.fork()

    if child_pid == 0:
        # This is the child process fork. Anything printed here will show up in the pty, including the output of this subprocess
        ganga_env = os.environ.copy()
        ganga_env["WEB_CLI"] = "True"
        ganga_env["INTERNAL_PORT"] = str(web_cli.config["INTERNAL_PORT"])
        subprocess.run(["ganga", "--webgui"], env=ganga_env)
    else:
        # This is the parent process fork. Store fd (connected to the child’s controlling terminal) and child pid
        web_cli.config["FD"] = fd
        web_cli.config["CHILD_PID"] = child_pid
        set_windowsize(fd, 50, 50)
        print("Ganga PID: ", child_pid)  # TODO


# Ping route to check if server is online
@web_cli.route("/ping")
def ping():
    return jsonify(True)


# Serve CLI
@web_cli.route("/cli")
def serve_cli():
    return render_template("cli.html")


# Redirect to CLI page
@web_cli.route("/", defaults={"path": ""})
@web_cli.route("/<path:path>")
def redirect_to_cli(path):
    return redirect(url_for("serve_cli"))


# Establish a websocket connection from the frontend to the server
@socketio.on("connect", namespace="/pty")
def connect():
    """
    New client connected
    """

    if web_cli.config["CHILD_PID"]:
        # Start background reading and emitting the output of the pseudo terminal
        socketio.start_background_task(target=read_and_forward_pty_output)
        return


# Input from the frontend
@socketio.on("pty-input", namespace="/pty")
def pty_input(data):
    """
    Write to the child pty. The pty sees this as if you are typing in a real terminal.
    """

    if web_cli.config["FD"]:
        os.write(web_cli.config["FD"], data["input"].encode())


# Resize the pseudo terminal when the frontend is resized
@socketio.on("resize", namespace="/pty")
def resize(data):
    if web_cli.config["FD"]:
        set_windowsize(web_cli.config["FD"], data["rows"], data["cols"])


# ******************** Helper Functions ******************** #


# Set the window size of the pseudo terminal according to the size in the frontend
def set_windowsize(fd, row, col, xpix=0, ypix=0):
    winsize = struct.pack("HHHH", row, col, xpix, ypix)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, winsize)


# Read and forward that data from the pseudo terminal to the frontend
def read_and_forward_pty_output():
    max_read_bytes = 1024 * 20
    while True:
        socketio.sleep(0.01)
        if web_cli.config["FD"]:
            timeout_sec = 0
            (data_ready, _, _) = select.select([web_cli.config["FD"]], [], [], timeout_sec)
            if data_ready:
                output = os.read(web_cli.config["FD"], max_read_bytes).decode()
                socketio.emit("pty-output", {"output": output}, namespace="/pty")


# ******************** Server Functions ******************** #


def start_web_cli(host: str, port: int, internal_port: int):
    """
    Start the web server on eventlet serving the terminal on the specified port. (Production ready server)
    :param host: str
    :param port: int
    :param internal_port: int
    """

    web_cli.config["INTERNAL_PORT"] = internal_port
    socketio.run(web_cli, host=host, port=port, log_output=True)  # TODO


# ******************** EOF ******************** #
