import os
import jwt
import json
import requests
import time
import select
import termios
import struct
import fcntl
import subprocess
import pty
import sys
import datetime
from functools import wraps
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from flask import Flask, request, jsonify, render_template, flash, redirect, url_for, session, send_file, make_response
from flask_login import login_user, login_required, logout_user, current_user, UserMixin
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from flask_socketio import SocketIO
from GangaGUI.gui.config import Config

# ******************** Initialisation of Flask App for GUI ******************** #


# GUI Flask App and set configuration from ./config.py file
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

# For websocket, for communication between frontend and backend
socketio = SocketIO(gui)


# ******************** The user class for database and authentication ******************** #

# ORM Class to represent Users - used to access the GUI & API resources
class User(UserMixin, db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    public_id = db.Column(db.String(64), unique=True)
    user = db.Column(db.String(32), unique=True)
    password_hash = db.Column(db.String(64))
    role = db.Column(db.String(32))
    pinned_jobs = db.Column(db.Text)

    def store_password_hash(self, password: str):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def generate_auth_token(self, expires_in_days: int = 5) -> str:
        return jwt.encode(
            {"public_id": self.public_id, "exp": datetime.datetime.utcnow() + datetime.timedelta(days=expires_in_days)},
            gui.config["SECRET_KEY"], algorithm="HS256")

    def __repr__(self):
        return "User {}: {} (Public ID: {}, Role: {})".format(self.id, self.user, self.public_id, self.role)


# User Loader Function for Flask Login
@login.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


# ******************** Global Variables ******************** #


# Colors showed for different job statuses in the GUI based on Bootstrap CSS
status_color = {
    "new": "info",
    "completed": "success",
    "completed_frozen" : "success",
    "failed": "danger",
    "failed_frozen" : "danger",
    "running": "primary",
    "submitted": "secondary",
    "killed": "warning"
}

# Allowed extensions when uploading any files to GUI
ALLOWED_EXTENSIONS = {"txt", "py"}

# Variables to globally store plugins and actions
actions = {}
plugins = {}


# ******************** Run Before First Request ******************** #


# Execute before first request
@gui.before_first_request
def initial_run():
    """
    This function runs before first request. It stores actions and plugins information from the ganga. It create default session cookies. If WEB_CLI is also started then it also starts a Ganga session.
    """

    global actions, plugins

    # Start ganga if WEB_CLI mode is True
    if gui.config['WEB_CLI'] is True:
        start_ganga(gui.config['INTERNAL_PORT'], args=gui.config["GANGA_ARGS"])
        session["WEB_CLI"] = True
    elif gui.config['INTERNAL_PORT'] is None:
        gui.config['INTERNAL_PORT'] = os.environ['INTERNAL_PORT']

    # If user is authenticated, log them out. This happens after a fresh start of the GUI server.
    if current_user.is_authenticated:
        logout_user()

    # Create user session defaults
    create_session_defaults()

    # Check if internal server is online, exit after 20s of retrying
    if not ping_internal():
        print("INTERNAL SERVER UNAVAILABLE, TERMINATING...")
        sys.exit(1)

    # Get job actions and plugins information from ganga
    try:
        # Get actions and plugins data once
        actions = query_internal_api("/internal/jobs/actions", "get")
        plugins = query_internal_api("/internal/plugins", "get")
    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))


# ******************** View Routes ******************** #


# Login View
@gui.route("/login", methods=["GET", "POST"])
def login():
    """
    Handles login route of the GUI.
    """

    # If already authenticated, logout
    if current_user.is_authenticated:
        return redirect(url_for("dashboard"))

    # Login user
    if request.method == "POST":

        # Form data
        username = request.form.get("username")
        password = request.form.get("password")

        # Database query
        user = User.query.filter_by(user=username).first()

        # If valid user, login
        if user and user.verify_password(password):
            login_user(user, True)
            flash("Login successful", "success")
            return redirect(url_for("dashboard"))

        flash("Error identifying the user", "danger")

    # Get users from the database
    users = User.query.all()

    return render_template("login.html", title="Login", users=users)


# Logout View
@gui.route("/logout", methods=["GET"])
def logout():
    """
    Logout user from GUI
    """

    # Logout
    if current_user.is_authenticated:
        logout_user()

    return redirect(url_for("login"))


# Dashboard view
@gui.route("/")
@login_required
def dashboard():
    """
    Handles the dashboard route of the GUI.
    """

    quick_statistics = {}
    recent_jobs_info = []
    pinned_jobs_info = []

    try:
        # Query overall statistics
        quick_statistics = query_internal_api("/internal/jobs/statistics", "get")

        # Query recent 10 jobs
        recent_jobs_info = query_internal_api("/internal/jobs/recent", "get")

        # Query pinned jobs
        u = current_user
        pinned_jobs_info = query_internal_api("/internal/jobs", "get", params={
            "ids": u.pinned_jobs if u.pinned_jobs is not None else json.dumps([]),
            "auto-validate-ids": True})

    except Exception as err:
        # Flash the error in the GUI
        flash(str(err), "danger")

    return render_template("dashboard.html",
                           title="Dashboard",
                           quick_statistics=quick_statistics,
                           recent_jobs_info=recent_jobs_info,
                           pinned_jobs_info=pinned_jobs_info,
                           status_color=status_color)


# Config view
@gui.route("/config", methods=["GET", "POST"])
@login_required
def config_page():
    """
    Handles the config route of the GUI.
    """

    full_config_info = []
    config_info = []
    section = None

    # When GUI request for specific section
    if request.method == "POST":
        # Get section name for request form data
        section = request.form.get("section")
        section = None if section in ["", None] else section

    try:
        # Query full config
        full_config_info = query_internal_api("/internal/config", "get")

        # If asked for specific section, add only that for displaying
        config_info = full_config_info if section is None else [s for s in full_config_info if s["name"] == section]

    except Exception as err:
        # Flash the error in the GUI
        flash(str(err), "danger")

    return render_template("config.html", title="Config", full_config_info=full_config_info, config_info=config_info)

#Edit gangarc
@gui.route("/config_edit",methods=["GET", "POST"])
@login_required
def edit_config_page():
    """
    Edit gangarc file from the GUI
    """
    gui_rc = gui.config["GANGA_RC"]
    with open(gui_rc, "rt") as f:
        ganga_config = f.read()
    if request.method == 'POST':
        config_ganga = request.form['config-data']
        with open(gui_rc, 'w') as f1:
            f1.write(str(config_ganga))
        flash(".gangarc Edited", "success")
        with open(gui_rc, "rt") as f2:
            ganga_config = f2.read()
    return render_template("config_edit.html", title="Edit gangarc", ganga_config=ganga_config)
    
@login_required
# Create view
@gui.route("/create", methods=["GET", "POST"])
def create_page():
    """
    Handles create route of the GUI.
    """

    # Handle file uploads
    if request.method == "POST":

        # Load from the uploaded file
        if "loadfile" in request.files:
            loadfile = request.files["loadfile"]
            if loadfile.filename == "":
                flash("No file selected", "warning")
                return redirect(request.url)

            # If valid file, the save the file
            if loadfile and allowed_file(loadfile.filename):
                save_path = os.path.join(gui.config["UPLOAD_FOLDER"], "loadfile.txt")
                loadfile.save(save_path)

                # Load the file
                try:
                    # Query to load the file
                    response_info = query_internal_api("/internal/load", "get", params={"path": save_path})

                except Exception as err:
                    # Display error in the GUI
                    flash(str(err), "danger")
                    return redirect(request.url)

                # Success message
                flash(response_info.get("message"), "success")
                return redirect(request.url)

        # Run file using the runfile GPI function
        if "runfile" in request.files:
            runfile = request.files["runfile"]
            if runfile.filename == "":
                flash("No file selected", "warning")
                return redirect(request.url)

            # If valid file, save the file
            if runfile and allowed_file(runfile.filename):
                save_path = os.path.join(gui.config["UPLOAD_FOLDER"], "runfile.py")
                runfile.save(save_path)

                # Run the file
                try:
                    # Query ganga to run the file
                    response_info = query_internal_api("/internal/runfile", "get", params={"path": save_path})

                except Exception as err:
                    # Display error back to GUI
                    flash(str(err), "danger")
                    return redirect(request.url)

                # Success message
                flash(response_info.get("message"), "success")
                return redirect(request.url)

        # No file case
        flash("No file, retry!", "warning")
        return redirect(request.url)

    try:
        # Query templates info
        templates_info = query_internal_api("/internal/templates", "get",
                                            params={"recent": True, "length": "6"})

    except Exception as err:
        # Display error to GUI
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))

    return render_template("create.html", title="Create", templates_info=templates_info)


# Runfile view
@gui.route("/create/runfile", methods=["GET", "POST"])
@login_required
def runfile_page():
    """
    Quick create a runfile to be run using the runfile GPI function.
    """

    # Runfile path
    runfile_path = os.path.join(gui.config["UPLOAD_FOLDER"], "runfile.py")

    # Save runfile data from frontend
    if request.method == "POST":
        runfile_data = request.form.get("runfile-data")
        with open(runfile_path, "w+") as f:
            f.write(runfile_data)

        # Run the file
        try:
            # Query ganga to run the file
            response_info = query_internal_api("/internal/runfile", "get", params={"path": runfile_path})
            flash(response_info["message"], "success")

        except Exception as err:
            # Display error back in the GUI
            flash(str(err), "danger")
            return redirect(request.url)

    return render_template("runfile.html", title="Runfile")


# Templates view
@gui.route("/templates", methods=["GET", "POST"])
@login_required
def templates_page():
    """
    Handles the templates route of the GUI. Displays templates in a tabular form.
    """

    # Update filter values
    if request.method == "POST":
        # Add filter data to user session
        session["templates_per_page"] = int(request.form.get("templates-per-page"))
        session["templates_filter"] = {key: request.form.get(form_name) for key, form_name in
                                       zip(["application", "backend"], ["template-application", "template-backend"])}

    # Current page
    current_page = int(request.args.get("page")) if request.args.get("page") is not None else 0

    # Get user defined value from session
    templates_per_page = session["templates_per_page"]

    try:

        # Query total number of templates
        templates_length = query_internal_api("/internal/templates/length", "get", params=session["templates_filter"])

        # Calculate number of max pages
        number_of_pages = (int(templates_length) // int(templates_per_page)) + 1

        # if current page exceeds last possible page, redirect to last page
        if current_page >= number_of_pages:
            return redirect(url_for("templates_page", page=number_of_pages - 1))

        # Add templates filters and range options for query params
        params = session["templates_filter"].copy()
        params.update({
            "recent": True,
            "length": templates_per_page,
            "offset": current_page
        })

        # Query templates information
        templates_info = query_internal_api("/internal/templates", "get", params=params)

    except Exception as err:
        # Flash error if any
        flash(str(err), "danger")
        return redirect(url_for("create_page"))

    return render_template("templates.html",
                           title="Templates",
                           number_of_pages=number_of_pages,
                           current_page=current_page,
                           backends=plugins["backends"],
                           applications=plugins["applications"],
                           templates_info=templates_info)


# Jobs view
@gui.route("/jobs", methods=["GET", "POST"])
@login_required
def jobs_page():
    """
    Handles jobs route of the GUI. Displays jobs in a tabular view.
    """

    # Update filter values
    if request.method == "POST":
        # Add form data to user session
        session["jobs_per_page"] = int(request.form.get("jobs-per-page"))
        session["jobs_filter"] = {key: request.form.get(form_name) for key, form_name in
                                  zip(["status", "application", "backend"],
                                      ["job-status", "job-application", "job-backend"])}

    # Current page
    current_page = int(request.args.get("page")) if request.args.get("page") is not None else 0

    # Get user defined value from user session
    jobs_per_page = session["jobs_per_page"]

    try:

        # Query total number of jobs
        jobs_length = query_internal_api("/internal/jobs/length", "get", params=session["jobs_filter"])

        # Calculate number of max pages
        number_of_pages = (int(jobs_length) // int(jobs_per_page)) + 1

        # if current page exceeds last possible page, redirect to last page
        if current_page >= number_of_pages:
            return redirect(url_for("jobs_page", page=number_of_pages - 1))

        # Add jobs filters and range options for query params
        params = session["jobs_filter"].copy()
        params.update({
            "recent": True,
            "length": jobs_per_page,
            "offset": current_page
        })

        # Query jobs information
        jobs_info = query_internal_api("/internal/jobs", "get", params=params)

    except Exception as err:
        # Display error back to GUI
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))

    return render_template("jobs.html",
                           title="Jobs",
                           jobs_info=jobs_info,
                           backends=plugins["backends"],
                           applications=plugins["applications"],
                           number_of_pages=number_of_pages,
                           current_page=current_page,
                           status_color=status_color)


# Job view
@gui.route('/jobs/<int:job_id>')
@login_required
def job_page(job_id: int):
    """
    Handles job route of the GUI. Displays all the information about the job.
    :param job_id: int
    """

    stdout = None
    stderr = None

    try:

        # Query job information
        job_info = query_internal_api(f"/internal/jobs/{job_id}", "get")

        # Query full print of the job
        full_print_info = query_internal_api(f"/internal/jobs/{job_id}/full-print", "get")

        # stdout and stderr path
        stdout_path = os.path.join(job_info["outputdir"], "stdout")
        stderr_path = os.path.join(job_info["outputdir"], "stderr")

        # Get stdout
        if os.path.exists(stdout_path):
            with open(stdout_path) as f:
                stdout = f.read()

        # Get stderr
        if os.path.exists(stderr_path):
            with open(stderr_path) as f:
                stderr = f.read()

    except Exception as err:
        # Display error on the GUI
        flash(str(err), "danger")
        return redirect(url_for("jobs_page"))

    return render_template("job.html",
                           title=f"Job {job_id}",
                           job_info=job_info,
                           status_color=status_color,
                           attribute_actions=actions.get("attributes"),
                           method_actions=actions.get("methods"),
                           stdout=stdout,
                           stderr=stderr,
                           full_print_info=full_print_info)


# Export job
@gui.route("/jobs/<int:job_id>/export")
@login_required
def job_export(job_id: int):
    """
    Sends the job file which is generated using export function of GPI.
    :param job_id: int
    """

    # Path to save file using export GPI function
    export_path = os.path.join(gui.config["UPLOAD_FOLDER"], f"export.txt")

    try:

        # Query to export the job at export path
        response_info = query_internal_api(f"/internal/jobs/{job_id}/export", "get", params={"path": export_path})

        # Send file
        return send_file(export_path, as_attachment=True, cache_timeout=0, attachment_filename=f"Job_{job_id}.txt")

    except Exception as err:
        # Display error back to GUI
        flash(str(err), "danger")

    return redirect(url_for("job_page", job_id=job_id))


# Edit job
@gui.route("/jobs/<int:job_id>/edit", methods=["GET", "POST"])
@login_required
def job_edit(job_id: int):
    """
    Show the exported job text on the GUI for it to be edited and submit. Will create a new job after submission.
    :param job_id: int
    """

    # Save paths
    loadfile_path = os.path.join(gui.config["UPLOAD_FOLDER"], "loadfile.txt")
    export_path = os.path.join(gui.config["UPLOAD_FOLDER"], "export.txt")

    # Create a new job with the submitted information
    if request.method == "POST":

        # Save the edited job info
        edited_job_info = request.form.get("edited-job-info")
        with open(loadfile_path, "w+") as f:
            f.write(edited_job_info)

        # Load the file
        try:
            # Query to load the job
            response_info = query_internal_api("/internal/load", "get", params={"path": loadfile_path})
            flash(response_info["message"], "success")

        except Exception as err:
            # Display error on the GUI
            flash(str(err), "danger")
            return redirect(request.url)

    try:
        # Query to export the job text
        response_info = query_internal_api(f"/internal/jobs/{job_id}/export", "get", params={"path": export_path})

        # Read exported job file to display
        with open(export_path) as f:
            exported_data = f.read()

    except Exception as err:
        # Display error on the GUI
        flash(str(err), "danger")
        return redirect(url_for("job_page", job_id=job_id))

    return render_template("edit_job.html", title=f"Edit Job {job_id}", job_id=job_id, exported_data=exported_data)


# Browse job directory
@gui.route("/job/<int:job_id>/browse", defaults={"path": ""})
@gui.route("/job/<int:job_id>/browse/<path:path>")
@login_required
def job_browse(job_id: int, path):
    """
    Browse directory of the job.
    :param job_id: int
    :param path: str
    """

    try:
        # Query job information
        job_info = query_internal_api(f"/internal/jobs/{job_id}", "get")

        # Base directory of the job
        job_base_dir = os.path.dirname(os.path.dirname(job_info["outputdir"]))

    except Exception as err:
        # Display error on the GUI
        flash(str(err), "danger")
        return redirect(url_for("job_page", job_id=job_id))

    # Join the base and the requested path
    abs_path = os.path.join(job_base_dir, path)

    # URL path variable for going back
    back_path = os.path.dirname(abs_path).replace(job_base_dir, "")

    # If path doesn't exist
    if not os.path.exists(abs_path):
        flash("Directory for this job does not exist.", "warning")
        return redirect(url_for("job_page", job_id=job_id))

    # Check if path is a file and send
    if os.path.isfile(abs_path):
        return send_file(abs_path)

    files_info = []

    # Show directory contents
    files = os.listdir(abs_path)

    # Store directory information
    for file in files:
        files_info.append({
            "file": file,
            "directory": os.path.isdir(os.path.join(abs_path, file))
        })

    return render_template('job_dir.html', title=f"Job {job_id} Directory",
                           job_id=job_id,
                           abs_path=abs_path,
                           files_info=files_info,
                           back_path=back_path)


# Subjobs view
@gui.route("/jobs/<int:job_id>/subjobs", methods=["GET", "POST"])
@login_required
def subjobs_page(job_id: int):
    """
    Handles subjobs view of the GUI. Displays subjobs of a job in a tabular form.
    :param job_id: int
    """

    # Change filter values
    if request.method == "POST":
        # Add form data to client session
        session["subjobs_per_page"] = int(request.form.get("subjobs-per-page"))
        session["subjobs_filter"] = {key: request.form.get(form_name) for key, form_name in
                                     zip(["status", "application", "backend"],
                                         ["subjob-status", "subjob-application", "subjob-backend"])}

    # Current page
    current_page = int(request.args.get("page")) if request.args.get("page") is not None else 0

    # Get user defined value from session
    subjobs_per_page = session["subjobs_per_page"]

    try:
        # Query total number of subjobs
        subjobs_length = query_internal_api(f"/internal/jobs/{job_id}/subjobs/length", "get",
                                            params=session["subjobs_filter"])

        # Calculate number of max pages
        number_of_pages = (int(subjobs_length) // int(subjobs_per_page)) + 1

        # if current page exceeds last possible page, redirect to last page
        if current_page >= number_of_pages:
            return redirect(url_for("subjobs_page", page=number_of_pages - 1, job_id=job_id))

        # Add subjobs filters and range options for query params
        params = session["subjobs_filter"].copy()
        params.update({
            "recent": True,
            "length": subjobs_per_page,
            "offset": current_page
        })

        # Query subjobs information
        subjobs_info = query_internal_api(f"/internal/jobs/{job_id}/subjobs", "get", params=params)

    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("job_page", job_id=job_id))

    return render_template("subjobs.html",
                           title=f"Subjobs - Job {job_id}",
                           status_color=status_color,
                           number_of_pages=number_of_pages,
                           current_page=current_page,
                           backends=plugins["backends"],
                           applications=plugins["applications"],
                           subjobs_info=subjobs_info,
                           job_id=job_id)


# Subjob view
@gui.route("/jobs/<int:job_id>/subjobs/<int:subjob_id>", methods=["GET"])
@login_required
def subjob_page(job_id: int, subjob_id: int):
    """
    Handles subjob route of the GUI. Displays extensive details of a subjob.
    :param job_id: int
    :param subjob_id: int
    """

    stdout = None
    stderr = None

    try:

        # Query job information
        job_outputdir = query_internal_api(f"/internal/jobs/{job_id}/outputdir", "get")

        # Query subjob information
        subjob_info = query_internal_api(f"/internal/jobs/{job_id}/subjobs/{subjob_id}", "get")

        # Query full print of the job
        full_print_info = query_internal_api(f"/internal/jobs/{job_id}/subjobs/{subjob_id}/full-print", "get")

        # Extract browse path that can be used by job_browse route
        job_dir_basepath = os.path.dirname(os.path.dirname(job_outputdir["outputdir"]))
        subjob_dir_basepath = os.path.dirname(os.path.dirname(subjob_info["outputdir"]))
        browse_path = subjob_dir_basepath.replace(job_dir_basepath, "")

        # stdout and stderr path
        stdout_path = os.path.join(subjob_info["outputdir"], "stdout")
        stderr_path = os.path.join(subjob_info["outputdir"], "stderr")

        # Get stdout
        if os.path.exists(stdout_path):
            with open(stdout_path) as f:
                stdout = f.read()

        # Get stderr
        if os.path.exists(stderr_path):
            with open(stderr_path) as f:
                stderr = f.read()

    except Exception as err:
        # Display error on the GUI
        flash(str(err), "danger")
        return redirect(url_for("subjobs_page", job_id=job_id))

    return render_template("subjob.html",
                           title=f"Subjob {subjob_id} - Job {job_id}",
                           subjob_info=subjob_info,
                           status_color=status_color,
                           attribute_actions=actions["attributes"],
                           method_actions=actions["methods"],
                           stdout=stdout,
                           stderr=stderr,
                           full_print_info=full_print_info,
                           job_id=job_id,
                           browse_path=browse_path)


# Credential view
@gui.route("/credentials")
@login_required
def credentials_page():
    """
    Handles credential store view of the GUI. Displays credentials in a tabular form.
    """

    try:
        # Query credential store information
        credentials_info = query_internal_api("/internal/credentials", "get")

    except Exception as err:
        # Display error on the GUI
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))

    return render_template('credentials.html', credential_info_list=credentials_info)

@gui.route("/queue", methods=["GET"])
@login_required
def queue_page():
    """
    Displays queues information
    """
    try:
        queue_info = query_internal_api("/internal/queue", "get")
    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))

    return render_template('queue.html', queue_info_list=queue_info)

# Plugins view
@gui.route('/plugins')
@login_required
def plugins_page():
    """
    Handles plugins route of the GUI. Displays the list of plugins.
    """

    try:
        # Store plugins information
        plugins_info = plugins

    except Exception as err:
        # Display error on the GUI
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))

    return render_template('plugins.html', plugins_info=plugins_info)


# Plugin view
@gui.route("/plugin/<plugin_name>")
@login_required
def plugin_page(plugin_name: str):
    """
    Displays information about the plugin like it's docstring.
    :param plugin_name: str
    """

    try:
        # Query plugin information
        plugin_info = query_internal_api(f"/internal/plugins/{plugin_name}", "get")

    except Exception as err:
        # Display error on the GUI
        flash(str(err), "danger")
        return redirect(url_for("plugins_page"))

    return render_template("plugin.html", title=f"{plugin_name}", plugin_info=plugin_info)


# Ganga logs view
@gui.route("/logs")
@login_required
def logs_page():
    """
    Diplay ganga log file.
    :return:
    """

    ganga_log_path = gui.config["GANGA_LOG"]
    gui_accesslog_path = gui.config["ACCESS_LOG"]
    gui_errorlog_path = gui.config["ERROR_LOG"]

    try:
        # Get ganga log
        with open(ganga_log_path, "rt") as f:
            ganga_log_data = f.read()

        # Get GUI access log
        with open(gui_accesslog_path, "rt") as f:
            gui_accesslog_data = f.read()

        # Get GUI error log
        with open(gui_errorlog_path, "rt") as f:
            gui_errorlog_data = f.read()

    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))

    return render_template("logs.html", title="Logs", ganga_log_data=ganga_log_data,
                           gui_accesslog_data=gui_accesslog_data, gui_errorlog_data=gui_errorlog_data)


@gui.route("/storage", defaults={"path": ""}, methods=["GET", "POST"])
@gui.route("/storage/<path:path>", methods=["GET", "POST"])
@login_required
def storage_page(path):
    """
    A convenience feature to store some file remotely in gangadir/storage
    """

    # Storage folder path
    storage_folder = gui.config["STORAGE_FOLDER"]

    # Join the storage path and the requested path
    abs_path = os.path.join(storage_folder, path)

    # Handle file uploads
    if request.method == "POST":

        # Uploaded file
        if "storagefile" in request.files:
            storagefile = request.files["storagefile"]
            if storagefile.filename == "":
                flash("No file selected", "warning")
                return redirect(request.url)

            # If valid file, the save the file
            if storagefile:
                # Directory check
                if not os.path.isdir(abs_path):
                    flash("Error while uploading the file", "danger")
                    return redirect(request.url)

                filename = secure_filename(storagefile.filename)
                save_path = os.path.join(abs_path, filename)
                storagefile.save(save_path)

                # Success message
                flash("Successfully uploaded the file.", "success")
                return redirect(request.url)

        # No file case
        flash("No file, retry!", "warning")
        return redirect(request.url)

    # URL path variable for going back
    back_path = os.path.dirname(abs_path).replace(storage_folder, "")

    # If path doesn't exist
    if not os.path.exists(abs_path):
        flash("Directory does not exist.", "warning")
        return redirect(url_for("dashboard"))

    # Check if path is a file and send
    if os.path.isfile(abs_path):
        return send_file(abs_path)

    files_info = []

    # Show directory contents
    files = os.listdir(abs_path)

    # Store directory information
    for file in files:
        files_info.append({
            "file": file,
            "directory": os.path.isdir(os.path.join(abs_path, file))
        })

    return render_template("storage.html", title="Storage",
                           abs_path=abs_path,
                           files_info=files_info,
                           back_path=back_path)


# Serve CLI
@gui.route("/cli")
@login_required
def serve_cli():
    return render_template("cli.html")


# Establish a websocket connection from the frontend to the server
@socketio.on("connect", namespace="/pty")
def connect():
    """
    New client connected, start reading and writing from the pseudo terminal.
    """

    if gui.config["CHILD_PID"] and current_user.is_authenticated:
        # Start background reading and emitting the output of the pseudo terminal
        socketio.start_background_task(target=read_and_forward_pty_output)
        return


# Input from the frontend
@socketio.on("pty-input", namespace="/pty")
def pty_input(data):
    """
    Write to the child pty. The pty sees this as if you are typing in a real terminal.
    """

    if gui.config["FD"] and current_user.is_authenticated:
        os.write(gui.config["FD"], data["input"].encode())


# Resize the pseudo terminal when the frontend is resized
@socketio.on("resize", namespace="/pty")
def resize(data):
    """
    Resize the pseudo terminal according to the dimension at the frontend.
    :param data: contains information about rows and cols of the frontend terminal.
    """

    if gui.config["FD"] and current_user.is_authenticated:
        set_windowsize(gui.config["FD"], data["rows"], data["cols"])


# ******************** Token Based Authentication ******************** #

# Generate token for API authentication - token validity 5 days
@gui.route("/token", methods=["POST"])
def generate_token():
    """
    Using the 'user' and 'password' data from the form body, validates the user and returns a JSON Web Token (JWT).
    """

    # Request form data
    request_json = request.json if request.json else {}
    request_user = request_json.get("username")
    request_password = request_json.get("password")

    # Handle no user or no password case
    if not request_user or not request_password:
        response_data = {"success": False, "message": "Could not verify user."}
        return jsonify(response_data), 401

    # Verify user and accordingly return the token
    user = User.query.filter_by(user=request_user).first()
    if user and user.verify_password(request_password):
        token = user.generate_auth_token().decode("UTF-8")
        response_data = {"token": token}
        return jsonify(response_data)

    # If authentication fails, return 401 HTTP code
    response_data = {"success": False, "message": "Could not verify user."}
    return jsonify(response_data), 401


# ******************** Token Authentication Decorator ******************** #

# Decorator for token protected routes
def token_required(f):
    """
    Decorator which validates the request header token in 'X-Acess-Token' field, and returns the user.
    """

    @wraps(f)
    def decorated(*args, **kwargs):

        token = None

        # Extract token from headers
        if "X-Access-Token" in request.headers:
            token = request.headers["X-Access-Token"]

        if not token:
            return jsonify({"success": False, "message": "Token is missing"}), 401

        # Decode the token and subsequently identify the user
        try:
            data = jwt.decode(token, gui.config["SECRET_KEY"], algorithms=["HS256"])
            current_api_user = User.query.filter_by(public_id=data["public_id"]).first()
            if current_api_user is None:
                return jsonify({"success": False, "message": "Token is old. Please renew"}), 401
        except jwt.ExpiredSignatureError:
            return jsonify({"success": False, "message": "Token is expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"success": False, "message": "Token is invalid"}), 401
        except:
            return jsonify({"success": False, "message": "Could not verify token"}), 401

        return f(current_api_user, *args, **kwargs)

    return decorated


# ******************** Job API ******************** #


# Single job information API - GET Method
@gui.route("/api/jobs/<int:job_id>", methods=["GET"])
@token_required
def job_endpoint(current_api_user, job_id: int):
    """
    Given the job_id, returns the general information related to the job in JSON format.

    :param job_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query job information to the GPI
        job_info = query_internal_api(f"/internal/jobs/{job_id}", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(job_info)


# Single job attribute information API - GET Method
@gui.route("/api/jobs/<int:job_id>/<attribute>", methods=["GET"])
@token_required
def job_attribute_endpoint(current_api_user, job_id: int, attribute: str):
    """
    Given the job_id and attribute, returns the attribute information in the JSON format.

    :param job_id: int
    :param attribute: str
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query job attribute information from ganga
        job_attribute_info = query_internal_api(f"/internal/jobs/{job_id}/{attribute}", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(job_attribute_info)


# Single job full print API - GET Method
@gui.route("/api/jobs/<int:job_id>/full-print", methods=["GET"])
@token_required
def job_full_print_endpoint(current_api_user, job_id: int):
    """
    Return full print of the job.

    :param job_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query job full print from ganga
        full_print_info = query_internal_api(f"/internal/jobs/{job_id}/full-print", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(full_print_info)


# Create job using template API - POST Method
@gui.route("/api/jobs/create", methods=["POST"])
@token_required
def job_create_endpoint(current_api_user):
    """
    Create a new job using the existing template.

    IMPORTANT: template_id NEEDS to be provided in the request body. job_name can optionally be provided in the request body.

    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    # Request data
    data = {
        "template_id": request.json.get("template_id"),
        "job_name": request.json.get("job_name")
    }

    try:
        # Query ganga to create a job using the template id
        response_info = query_internal_api("/internal/jobs/create", "post", json=data)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_info)


# Copy job API - PUT Method
@gui.route("/api/jobs/<int:job_id>/copy", methods=["PUT"])
@token_required
def job_copy_endpoint(current_api_user, job_id: int):
    """
    Create a copy of the job.
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    :param job_id: int
    """

    try:
        # Query ganga to copy the job
        response_info = query_internal_api(f"/internal/jobs/{job_id}/copy", "put")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_info)


# Job action API - PUT Method
@gui.route("/api/jobs/<int:job_id>/<action>", methods=["PUT"])
@token_required
def job_action_endpoint(current_api_user, job_id: int, action: str):
    """
    Given the job_id and action in the endpoint, perform the action on the job.

    The action can be any method or attribute change that can be called on the Job object.

    Example:
    1)
        PUT http://localhost:5000/job/13/resubmit

        The above request will resubmit the job with ID 13.

    2)
        PUT http://localhost:5000/job/13/force_status
        {"force_status":"failed"}

        The above request will force status of the job with ID 13 to killed. If unsuccessful will return back the error.

    3)
        PUT http://localhost:5000/job/13/name
        {"name"="New Name"}

        The above request will change the name of the job with ID 13 to "New Name". Notice how the required values
        are passed in the request body with the same name as action.

    NOTE: It is NECESSARY to send body in JSON format for the request to be parsed in JSON.

    :param job_id: int
    :param action: str
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    # Request data
    request_data = request.json

    try:
        # Query ganga to perform the action
        response_info = query_internal_api(f"/internal/jobs/{job_id}/{action}", "put", json=request_data)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_info)


# Job delete API - DELETE Method
@gui.route("/api/jobs/<int:job_id>", methods=["DELETE"])
@token_required
def job_delete_endpoint(current_api_user, job_id: int):
    """
    Given the job id, removes the job from the job repository.

    :param job_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to delete the job
        response_info = query_internal_api(f"/internal/jobs/{job_id}", "delete")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_info)


# Pin the Job
@gui.route("/api/jobs/<int:job_id>/pin", methods=["PUT"])
@token_required
def job_pin_endpoint(current_api_user, job_id: int):
    """
    Pin the given job, which is then shown in the dashboard.
    :param job_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    # Get current user
    u = current_user

    # Load pinned jobs of the user from the database
    pinned_jobs = json.loads(u.pinned_jobs) if u.pinned_jobs is not None else []

    # Pin job
    if job_id not in pinned_jobs:
        pinned_jobs.append(job_id)

    # Add new pinned jobs to the database
    u.pinned_jobs = json.dumps(pinned_jobs)
    db.session.add(u)
    db.session.commit()

    return jsonify({"success": True, "message": f"Successfully pinned Job (ID={job_id})."})


# Unpin the job
@gui.route("/api/jobs/<int:job_id>/unpin", methods=["PUT"])
@token_required
def job_unpin_endpoint(current_api_user, job_id: int):
    """
    Unpin the job, and make the required change to the GUI database.
    :param job_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    # Get the user from the database
    u = current_user

    # Load user's pinned job from the database
    pinned_jobs = json.loads(u.pinned_jobs) if u.pinned_jobs is not None else []

    # Unpin the job
    if job_id in pinned_jobs:
        pinned_jobs.remove(job_id)

    # Commit changes to the database
    u.pinned_jobs = json.dumps(pinned_jobs)
    db.session.add(u)
    db.session.commit()

    return jsonify({"success": True, "message": f"Successfully unpinned Job (ID={job_id})."})


# ******************** Subjobs API ******************** #

# Subjobs API - GET Method
@gui.route("/api/jobs/<int:job_id>/subjobs", methods=["GET"])
@token_required
def subjobs_endpoint(current_api_user, job_id: int):
    """
    Returns a list subjobs of a particular job in a similar way as Jobs API.

    The parameter accepted are:
    * ids: provide a JSON string of list of IDs
    * status: provide subjob status as a string for filter
    * application: provide subjob application as a string for filter
    * backend: provide backend application as a string for filter
    * recent: if provided, starts a list from recent subjobs to old
    * length: number of subjobs to be returned, provide as a int
    * offset: how many subjobs to skip before returning the specified length of subjobs. Provide as int.
        offset works as: number of subjobs skipped = offset * length

    :param job_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    params = {
        "ids": request.args.get("ids"),
        "status": request.args.get("status"),
        "application": request.args.get("application"),
        "backend": request.args.get("backend"),
        "recent": request.args.get("recent"),
        "length": request.args.get("length"),
        "offset": request.args.get("offset")
    }

    try:
        # Query ganga for subjobs information
        subjobs_info = query_internal_api(f"/internal/jobs/{job_id}/subjobs", "get", params=params)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(subjobs_info)


# Single subjob info API - GET Method
@gui.route("/api/jobs/<int:job_id>/subjobs/<int:subjob_id>", methods=["GET"])
@token_required
def subjob_endpoint(current_api_user, job_id: int, subjob_id: int):
    """
    Returns information of a single subjob related to a particular job

    :param job_id: int
    :param subjob_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query subjob information to ganga
        subjob_info = query_internal_api(f"/internal/jobs/{job_id}/subjobs/{subjob_id}", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(subjob_info)


# Single Subjob Attribute Info API - GET Method
@gui.route("/api/jobs/<int:job_id>/subjobs/<int:subjob_id>/<attribute>", methods=["GET"])
@token_required
def subjob_attribute_endpoint(current_api_user, job_id: int, subjob_id: int, attribute: str):
    """
    Given the job id, subjob id and attribute; return the attribute information in the string format via JSON.

    :param job_id: int
    :param subjob_id: int
    :param attribute: str
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query attribute information from ganga
        subjob_attribute_info = query_internal_api(f"/internal/jobs/{job_id}/subjobs/{subjob_id}/{attribute}", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(subjob_attribute_info)


# Single subjob full print API - GET Method
@gui.route("/api/jobs/<int:job_id>/subjobs/<int:subjob_id>/full-print", methods=["GET"])
@token_required
def subjob_full_print_endpoint(current_api_user, job_id: int, subjob_id: int):
    """
    Return full print of the subjob.

    :param subjob_id: int
    :param job_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query subjob full print from ganga
        full_print_info = query_internal_api(f"/internal/jobs/{job_id}/subjobs/{subjob_id}/full-print", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(full_print_info)


# Copy subjob API - PUT Method
@gui.route("/api/jobs/<int:job_id>/subjobs/<int:subjob_id>/copy", methods=["PUT"])
@token_required
def subjob_copy_endpoint(current_api_user, job_id: int, subjob_id: int):
    """
    Create a copy of the subjob into a new job.
    :param job_id:
    :param subjob_id:
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to copy subjob
        response_info = query_internal_api(f"/internal/jobs/{job_id}/subjobs/{subjob_id}/copy", "put")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_info)


# ******************** Jobs API ******************** #

# Jobs API - GET Method
@gui.route("/api/jobs", methods=["GET"])
@token_required
def jobs_endpoint(current_api_user):
    """
    Returns a list of jobs with general information in JSON format.

    The parameter accepted are:
    * ids: provide a JSON string of list of IDs
    * status: provide job status as a string for filter
    * application: provide job application as a string for filter
    * backend: provide backend application as a string for filter
    * recent: if provided, starts a list from recent job to old
    * length: number of job to be returned, provide as a int
    * offset: how many job to skip before returning the specified length of job. Provide as int.
        offset works like: number of job skipped = offset * length
    * auto-validate-ids: If ids provided in ids parameters does not exist in job repository, then skip those ids.

    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    params = {
        "ids": request.args.get("ids"),
        "status": request.args.get("status"),
        "application": request.args.get("application"),
        "backend": request.args.get("backend"),
        "recent": request.args.get("recent"),
        "length": request.args.get("length"),
        "offset": request.args.get("offset"),
        "auto-validate-ids": request.args.get("auto-validate-ids")
    }

    try:
        # Get jobs information according to select filter and range filter
        jobs_info = query_internal_api(f"/internal/jobs", "get", params=params)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(jobs_info)


# Jobs statistics API - GET Method
@gui.route("/api/jobs/statistics", methods=["GET"])
@token_required
def jobs_statistics_endpoint(current_api_user):
    """
    Returns the number of jobs in new, running, completed, killed, failed status.
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to get statistics information
        statistics = query_internal_api("/internal/jobs/statistics", "get")
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(statistics)

@gui.route("/api/queue", methods=["GET"])
@token_required
def queue_endpoint(current_api_user):

    try:
        queue_info = query_internal_api("/internal/queue", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(queue_info)

@gui.route("/api/queue/chart", methods=["GET","POST"])
def queue_chart_endpoint():

    
    chart_info = query_internal_api("/internal/queue/data", "get")
    response = make_response(json.dumps(chart_info))
    response.content_type = 'application/json'
    return response

# Job incomplete ids API - GET Method
@gui.route("/api/jobs/incomplete_ids", methods=["GET"])
@token_required
def jobs_incomplete_ids_endpoint(current_api_user):
    """
    Returns a list of incomplete job ids in JSON format.
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to get incomplete ids list
        incomplete_ids_list = query_internal_api("/internal/jobs/incomplete-ids", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(incomplete_ids_list)


# ******************** Config API ******************** #

# Config API - GET Method
@gui.route("/api/config", methods=["GET"], defaults={"section": ""})
@gui.route("/api/config/<section>", methods=["GET"])
@token_required
def config_endpoint(current_api_user, section: str):
    """
    Returns a list of all the section of the configuration and their options as well as the values in JSON format.

    If section is provide, returns information about the section in JSON format.

    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to get config information
        if section != "":
            config_info = query_internal_api(f"/internal/config/{section}", "get")
        else:
            config_info = query_internal_api("/internal/config", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(config_info)


# ******************** Templates API ******************** #

# Templates API - GET Method
@gui.route("/api/templates", methods=["GET"])
@token_required
def templates_endpoint(current_api_user):
    """
    Returns a list of objects containing template info in JSON format.

    * ids: provide a JSON string of list of IDs
    * status: provide template status as a string for filter
    * application: provide template application as a string for filter
    * backend: provide backend application as a string for filter
    * recent: if provided, starts a list from recent template to old
    * length: number of template to be returned, provide as a int
    * offset: how many template to skip before returning the specified length of template. Provide as int.
        offset works like: number of template skipped = offset * length

    :param current_api_user: Information of the current_user based on the request's JWT token
    """

    params = {
        "application": request.args.get("application"),
        "backend": request.args.get("backend"),
        "recent": request.args.get("recent"),
        "length": request.args.get("length"),
        "offset": request.args.get("offset"),
    }

    try:
        # Query ganga for templates information
        templates_info = query_internal_api("/internal/templates", "get", params=params)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(templates_info)


# Single template full print API - GET Method
@gui.route("/api/templates/<int:template_id>/full-print", methods=["GET"])
@token_required
def template_full_print_endpoint(current_api_user, template_id: int):
    """
    Return full print of the template.

    :param template_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query template full print from ganga
        full_print_info = query_internal_api(f"/internal/templates/{template_id}/full-print", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(full_print_info)


# Template API - DELETE Method
@gui.route("/api/templates/<int:template_id>", methods=["DELETE"])
@token_required
def delete_template_endpoint(current_api_user, template_id: int):
    """

    Given the templates id, delete it from the template repository.

    :param template_id: int
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to remove the template
        response_info = query_internal_api(f"/internal/templates/{template_id}", "delete")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_info)


# ******************** Credentials API ******************** #

# Credential store API - GET Method
@gui.route("/api/credentials", methods=["GET"])
@token_required
def credentials_endpoint(current_api_user):
    """
    Return a list of credentials and their information in JSON format.

    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga for credentials information
        credentials_info = query_internal_api("/internal/credentials", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(credentials_info)


# Credential Store API - PUT Method - Renew all credentials
@gui.route("/api/credentials/renew", methods=["PUT"])
@token_required
def credentials_renew_endpoint(current_api_user):
    """
    Renew all the credentials in the credential store.

    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to renew credentials
        response_info = query_internal_api("/internal/credentials/renew", "put")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_info)


# ******************** Job Tree API ******************** #

# Job tree API - GET Method
@gui.route("/api/jobtree", methods=["GET"])
@token_required
def jobtree_endpoint(current_api_user):
    """
    Return the job tree folder structure as the json format of python dict.

    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to get job tree information
        jobtree_info = query_internal_api("/internal/jobtree", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(jobtree_info)


# ******************** Job Tree API ******************** #

# Plugins API - GET Method
@gui.route("/api/plugins", methods=["GET"])
@token_required
def plugins_endpoint(current_api_user):
    """
    Return plugins information, category and names of the plugins in the category.

    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to get plugins information
        plugins_info = query_internal_api("/internal/plugins", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(plugins_info)


# Plugin API - GET Method
@gui.route("/api/plugins/<plugin_name>", methods=["GET"])
@token_required
def plugin_endpoint(current_api_user, plugin_name: str):
    """
    Return single plugin information like name and docstring.

    :param plugin_name: str
    :param current_api_user: Information of the current_api_user based on the request's JWT token
    """

    try:
        # Query ganga to get plugin information
        plugin_info = query_internal_api(f"/internal/plugins/{plugin_name}", "get")

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(plugin_info)


# ******************** Helper Functions ******************** #

# Validate uploaded filename.
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# Make HTTP request to the Internal Flask Server which is running on a GangaThread which has access to ganga namespace.
def query_internal_api(route: str, method: str, **kwargs):
    """
    :param route: str
    :param method: str
    :param kwargs: dict
    :return: dict

    Make a HTTP request to the Internal API Flask server which runs on a GangaThread to query data from Ganga.
    Check response status code and extract the data or raise an exception accordingly.

    kwargs can be param, json, etc. Any attribute that is supported by the requests module.
    """

    # Internal url for communicating with API server running on a GangaThread
    INTERNAL_URL = f"http://localhost:{gui.config['INTERNAL_PORT']}"

    # Raise error if HTTP method not supported
    if method not in ["get", "post", "put", "delete"]:
        raise Exception(f"Unsupported method: {method}")

    # Made the HTTP requests, along with whatever arguments provided
    res = getattr(requests, method)(INTERNAL_URL + route, **kwargs)

    # Check is request is OK
    if res.status_code != 200:
        raise Exception(res.json().get("message"))

    # Return request data
    return res.json()


def create_session_defaults():
    """
    Create user session defaults and assign default values to them.
    """

    # Set session defaults for templates filter
    if "templates_per_page" not in session:
        session["templates_per_page"] = 10
    if "templates_filter" not in session:
        session["templates_filter"] = {key: "any" for key in ["application", "backend"]}

    # Set session defaults for jobs filter
    if "jobs_per_page" not in session:
        session["jobs_per_page"] = 10
    if "jobs_filter" not in session:
        session["jobs_filter"] = {key: "any" for key in ["status", "application", "backend"]}

    # Set session defaults for subjobs filter
    if "subjobs_per_page" not in session:
        session["subjobs_per_page"] = 10
    if "subjobs_filter" not in session:
        session["subjobs_filter"] = {key: "any" for key in ["status", "application", "backend"]}


# Ping internal API server
def ping_internal():
    """
    Ping internal API server if it is running
    """

    trials = 0
    while True:
        try:
            ping = query_internal_api("/ping", "get")
            if ping is True:
                return True
        except:
            time.sleep(2)

        print("Internal API server not online (mostly because Ganga is booting up), retrying...")
        trials += 1
        if trials > 20:
            return False


def start_ganga(internal_port: int, args: str = ""):
    """
    Start a ganga session in a pseudo terminal and stores the file descriptor of the terminal as well as the PID of the ganga session.
    :param args: str - str of arguments to provide to ganga
    :param internal_port: int
    """

    # Create child process attached to a pty that we can read from and write to
    (child_pid, fd) = pty.fork()

    if child_pid == 0:
        # This is the child process fork. Anything printed here will show up in the pty, including the output of this subprocess
        ganga_env = os.environ.copy()
        ganga_env["WEB_CLI"] = "True"
        ganga_env["INTERNAL_PORT"] = str(internal_port)
        subprocess.run(f"ganga --webgui {args}", shell=True, env=ganga_env)
    else:
        # This is the parent process fork. Store fd (connected to the child’s controlling terminal) and child pid
        gui.config["FD"] = fd
        gui.config["CHILD_PID"] = child_pid
        set_windowsize(fd, 50, 50)
        print("Ganga started, PID: ", child_pid)


# Set the window size of the pseudo terminal according to the size in the frontend
def set_windowsize(fd, row, col, xpix=0, ypix=0):
    winsize = struct.pack("HHHH", row, col, xpix, ypix)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, winsize)


# Read and forward that data from the pseudo terminal to the frontend
def read_and_forward_pty_output():
    max_read_bytes = 1024 * 20
    while True:
        socketio.sleep(0.01)
        if gui.config["FD"]:
            timeout_sec = 0
            (data_ready, _, _) = select.select([gui.config["FD"]], [], [], timeout_sec)
            if data_ready:
                output = os.read(gui.config["FD"], max_read_bytes).decode()
                socketio.emit("pty-output", {"output": output}, namespace="/pty")


def start_web_cli(host: str, port: int, internal_port: int, log_output=True, ganga_args: str = ""):
    """
    Start the web server on eventlet serving the terminal on the specified port. (Production ready server)
    :param ganga_args: str - arguments to be passed to ganga
    :param host: str
    :param port: int
    :param internal_port: int
    """

    from GangaGUI.start import create_default_user

    # Create default user
    gui_user, gui_password = create_default_user()

    print(f"Starting the GUI server on http://{host}:{port}")
    print(f"You login information for the GUI is: Username: {gui_user.user} Password: {gui_password}")

    gui.config["INTERNAL_PORT"] = internal_port
    gui.config["WEB_CLI"] = True
    gui.config["GANGA_ARGS"] = ganga_args
    socketio.run(gui, host=host, port=port, log_output=log_output)  # TODO


# ******************** Shutdown Function ******************** #

# Route used to shutdown the Internal API server and GUI server
@gui.route("/shutdown", methods=["GET"])
def shutdown():

    if gui.config["WEB_CLI"] is True:
        flash("WEB CLI Mode is on, cannot self shutdown server. Consider doing manually.", "warning")
        return redirect(url_for("dashboard"))

    try:
        response_info = query_internal_api("/shutdown", "get")
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return "GUI Shutdown Successful."

# ******************** EOF ******************** #
