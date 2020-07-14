# ******************** Imports ******************** #

import jwt
import json
from functools import wraps
from itertools import chain
from flask import request, jsonify, render_template, flash
from GangaGUI.gui import app
from GangaGUI.gui.models import User


# ******************** View Routes ******************** #

# Dashboard route
@app.route("/")
def dashboard():
    from GangaCore.GPI import jobs

    # Get last 10 jobs slice
    recent_jobs = list(jobs[-10:])

    status_color = {"new": "info", "completed": "success", "failed": "danger", "running": "primary",
                    "submitted": "secondary"}

    return render_template("home.html", title="Dashboard", status_color=status_color, recent_jobs=recent_jobs,
                           jobs=jobs)


@app.route("/config", methods=["GET", "POST"])
def config():

    from GangaCore.GPI import config
    from GangaCore import getConfig

    sections = []
    config_list = []

    for c in config:
        config_list.append(c)

    if request.method == "POST":
        sectionName = request.form.get("section")
        if sectionName is not None:
            section = getConfig(str(sectionName))
            sections.append(section)
            return render_template("config.html", title="Config", sections=sections, configList=config_list)
        else:
            flash("Please select a config section to view.", "warning")

    for c in config_list:
        section = getConfig(c)
        sections.append(section)

    return render_template("config.html", title="Config", sections=sections, configList=config_list)


# ******************** Token Based Authentication ******************** #

# Generate token for API authentication - token validity 5 days
@app.route("/token", methods=["POST"])
def generate_token():
    """
    Using the 'user' and 'password' data from the form body, validates the user and returns a JSON Web Token (JWT).
    """

    # Request form data
    request_user = request.form.get("user")
    request_password = request.form.get("password")

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
            data = jwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])
            current_user = User.query.filter_by(public_id=data["public_id"]).first()
            if current_user is None:
                return jsonify({"success": False, "message": "Token is old. Please renew"}), 401
        except jwt.ExpiredSignatureError:
            return jsonify({"success": False, "message": "Token is expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"success": False, "message": "Token is invalid"}), 401
        except:
            return jsonify({"success": False, "message": "Could not verify token"}), 401

        return f(current_user, *args, **kwargs)

    return decorated


# ******************** Job API ******************** #


# Single Job Information API - GET Method
@app.route("/api/job/<int:job_id>", methods=["GET"])
@token_required
def job_endpoint(current_user, job_id: int):
    """
    Given the job_id, returns the general information related to the job in JSON format.

    Returned job information: fqid, status, name, number of subjobs, application, backend, backend.actualCE, comments, subjobs statuses.

    :param job_id: int
    :param current_user: Information of the current_user based on the request's JWT token
    """

    try:
        # Get the general info of the job
        job_info = get_job_info(job_id)
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(job_info)


# Single Job Attribute Info API - GET Method
@app.route("/api/job/<int:job_id>/<attribute>", methods=["GET"])
@token_required
def job_attribute_endpoint(current_user, job_id: int, attribute: str):
    """
    Given the job_id and attribute, returns the attribute information in the JSON string format.

    :param job_id: int
    :param attribute: str
    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import jobs

    try:
        j = jobs[job_id]
        response_data = {attribute: str(getattr(j, attribute))}
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_data)


# Create Job Using Template API - POST Method
@app.route("/api/job/create", methods=["POST"])
@token_required
def job_create_endpoint(current_user):
    """
    Create a new job using the existing template.

    IMPORTANT: template_id NEEDS to be provided in the request body. job_name can optionally be provided in the request body.

    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import templates, Job

    # Store request data
    template_id: int = request.form.get("template_id")
    job_name: str = request.form.get("job_name")

    try:
        # Create job using template
        j = Job(templates[int(template_id)])
        if job_name is not None:
            j.name = job_name
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True,
                    "message": "Job with ID {} created successfully using the template with ID {}".format(j.id,
                                                                                                          template_id)})


# Perform Certain Action on the Job - PUT Method
@app.route("/api/job/<int:job_id>/<action>", methods=["PUT"])
@token_required
def job_action_endpoint(current_user, job_id: int, action: str):
    """
    Given the job_id and action in the endpoint, perform the action on the job.

    The action can be any method or attribute change that can be called on the Job object.

    Example:
    1)
        PUT http://localhost:5000/job/13/resubmit

        The above request will resubmit the job with ID 13.

    2)
        PUT http://localhost:5000/job/13/force_status
        force_status="failed"

        The above request will force status of the job with ID 13 to killed. If unsuccessful will return back the error.

    3)
        PUT http://localhost:5000/job/13/name
        name="New Name"

        The above request will change the name of the job with ID 13 to "New Name". Notice how the required values
        are passed in the request body with the same name as action.

    NOTE: It is NECESSARY to wrap a String value in double quotes ("") when provide a value in the request body.
    Whereas DON'T wrap Integer or Boolean value in double quotes (""). Because values in double quotes are parsed as
    String, numerical values without double quotes as Integer/Float and True/False without double quotes as Boolean.
    You can also pass array of arguments. Eg. ["Test", 13, True] will be parsed as is in Python.

    :param job_id: int
    :param action: str
    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import jobs
    from GangaCore.GPIDev.Lib.Job import Job

    # Store request data in a dictionary
    request_data = request.form.to_dict()

    # Action validity check
    if action not in chain(Job._exportmethods, Job._schema.allItemNames()):
        return jsonify({"success": False, "message": f"{action} not supported or does not exist"}), 400

    # Action on Job Methods
    if action in Job._exportmethods:
        try:
            j = jobs(job_id)

            # Check for arguments in the request body for passing in the method
            if action in request_data.keys():
                args = json.loads(request_data[action])
                if isinstance(args, type([])):
                    getattr(j, action)(*args)
                else:
                    getattr(j, action)(args)
            else:
                getattr(j, action)()
        except Exception as err:
            return jsonify({"success": False, "message": str(err)}), 400

    if action in Job._schema.allItemNames():
        try:
            j = jobs(job_id)

            # Check for the value to set in the request body
            if action in request_data.keys():
                arg = json.loads(request_data[action])
                setattr(j, action, arg)
            else:
                return jsonify(
                    {"success": False, "message": f"Please provide the value for {action} in the request body"}), 400
        except Exception as err:
            return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(
        {"success": True, "message": f"Successfully completed the action {action} on the Job with ID {job_id}"})


# Delete Job API - DELETE Method
@app.route("/api/job/<int:job_id>", methods=["DELETE"])
@token_required
def delete_job_endpoint(current_user, job_id: int):
    """
    Given the job id, removes the job from the job repository.

    :param job_id: int
    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import jobs

    try:
        j = jobs[job_id]
        j.remove()
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Job with ID {} removed successfully".format(job_id)})


# ******************** Subjobs API ******************** #

# Subjobs API - GET Method
@app.route("/api/job/<int:job_id>/subjobs", methods=["GET"])
@token_required
def subjobs_endpoint(current_user, job_id: int):
    """
    Returns a list subjobs of a particular job in a similar way as Jobs API.
    
    :param job_id: int
    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import jobs

    try:
        j = jobs(int(job_id))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    # Store subjobs information in a list
    subjobs_info_list = []
    try:
        for sj in j.subjobs:
            subjobs_info_list.append(get_subjob_info(job_id=j.id, subjob_id=sj.id))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(subjobs_info_list)


# Single Subjob Info API - GET Method
@app.route("/api/job/<int:job_id>/subjob/<int:subjob_id>", methods=["GET"])
@token_required
def subjob_endpoint(current_user, job_id: int, subjob_id: int):
    """
    Returns information of a single subjob related to a particular job

    Returned information: id, fqid, status, name, application, backend, backend.actualCE, comment

    :param job_id: int
    :param subjob_id: int
    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import jobs

    # Using the job id and subjob id get the subjob info
    try:
        j = jobs(int(job_id))
        sj = j.subjobs[int(subjob_id)]
        response_data = get_subjob_info(job_id=j.id, subjob_id=sj.id)
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_data)


# Single Subjob Attribute Info API - GET Method
@app.route("/api/job/<int:job_id>/subjob/<int:subjob_id>/<attribute>", methods=["GET"])
@token_required
def subjob_attribute_endpoint(current_user, job_id: int, subjob_id: int, attribute: str):
    """
    Given the job id, subjob id and attribute; return the attribute information in the string format via JSON.

    :param job_id: int
    :param subjob_id: int
    :param attribute: str
    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import jobs

    # Get subjob attribute info
    try:
        j = jobs[int(job_id)]
        sj = j.subjobs[int(subjob_id)]
        response_data = {attribute: str(getattr(sj, attribute))}
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_data)


# ******************** Jobs API ******************** #

# Jobs API - GET Method
@app.route("/api/jobs", methods=["GET"])
@token_required
def jobs_endpoint(current_user):
    """
    Returns a list of jobs with general information in JSON format.
    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import jobs

    # Store job information in a list
    job_info_list = []
    try:
        for j in jobs:
            job_info_list.append(get_job_info(j.id))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(job_info_list)


# Job IDs API - GET Method
@app.route("/api/jobs/ids", methods=["GET"])
@token_required
def jobs_ids_endpoint(current_user):
    """
    Returns a list of job ids present in job repository.

    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import jobs

    # IDs list
    try:
        ids_list = list(jobs.ids())
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(ids_list)


# Job Incomplete IDs API - GET Method
@app.route("/api/jobs/incomplete_ids", methods=["GET"])
@token_required
def jobs_incomplete_ids_endpoint(current_user):
    """
    Returns a list of incomplete job ids in JSON format.
    """

    from GangaCore.GPI import jobs

    # Incomplete IDs list
    try:
        incomplete_ids_list = list(jobs.incomplete_ids())
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(incomplete_ids_list)


# ******************** Config API ******************** #

# Config API - GET Method
@app.route("/api/config", methods=["GET"])
@token_required
def config_endpoint(current_user):
    """
    Returns a list of all the section of the configuration and their options as well as the values in JSON format.

    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import config
    from GangaCore.Utility.Config import getConfig

    # To store sections of config
    list_of_sections = []

    # Get each section information and append to the list
    for section in config:

        config_section = getConfig(section)
        options_list = []

        # Get options information for the particular config section
        for o in config_section.options.keys():
            options_list.append({
                "name": str(config_section.options[o].name),
                "value": str(config_section.options[o].value),
                "docstring": str(config_section.options[o].docstring),
            })

        # Append config section data to the list
        list_of_sections.append({
            "name": str(config_section.name),
            "docstring": str(config_section.docstring),
            "options": options_list,
        })

    return jsonify(list_of_sections)


# ******************** Templates API ******************** #

# Templates API - GET Method
@app.route("/api/templates", methods=["GET"])
@token_required
def templates_endpoint(current_user):
    """
    Returns a list of objects containing template info in JSON format.

    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import templates

    # Store templates info in a list
    templates_info_list = []
    try:
        for t in templates:
            templates_info_list.append(get_template_info(t.id))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(templates_info_list)


# Template API - DELETE Method
@app.route("/api/template/<int:template_id>", methods=["DELETE"])
@token_required
def delete_template_endpoint(current_user, template_id: int):
    """

    Given the templates id, delete it from the template repository.

    :param template_id: int
    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import templates

    # Remove template
    try:
        t = templates[template_id]
        t.remove()
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Template with ID {} removed successfully".format(template_id)})


# ******************** Credential Store API ******************** #

# Credential Store API - GET Method - Get list of all credentials
@app.route("/api/credential_store", methods=["GET"])
@token_required
def credential_store_endpoint(current_user):
    """
    Return a list of credentials and their information in JSON format.

    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import credential_store

    # Store credential store info in a list
    credential_info_list = []
    try:
        for c in credential_store:
            credential_info = {}
            credential_info["location"] = str(c.location)
            credential_info["time_left"] = str(c.time_left())
            credential_info["expiry_time"] = str(c.expiry_time())
            credential_info["is_valid"] = str(c.is_valid())
            credential_info["exists"] = str(c.exists())
            credential_info_list.append(credential_info)
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(credential_info_list)


# Credential Store API - PUT Method - Renew all credentials
@app.route("/api/credential_store/renew", methods=["PUT"])
@token_required
def renew_credentials_endpoint(current_user):
    """
    Renew all the credentials in the credential store.
    """

    from GangaCore.GPI import credential_store

    try:
        credential_store.renew()
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Credentials store credentials renewed"})


# ******************** Job Tree API ******************** #

# Job Tree API - GET Method
@app.route("/api/jobtree", methods=["GET"])
@token_required
def jobtree_endpoint(current_user):
    """
    Return the job tree folder structure as the json format of python dict.

    :param current_user: Information of the current_user based on the request's JWT token
    """

    from GangaCore.GPI import jobtree

    try:
        # Reset job tree to root of job repository
        jobtree.cd()

        # Return the jobtree folder structure
        return jsonify(jobtree.folders)
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400


# ******************** Helper Functions ******************** #

def get_job_info(job_id: int) -> dict:
    """
    Given the job_id, return a dict containing
    [id, fqid, status, name, subjobs, application, backend, backend.actualCE, comments, subjob_statuses] info of the job.

    :param job_id: int
    :return: dict
    """

    from GangaCore.GPI import jobs

    j = jobs[int(job_id)]

    # Store job info in a dict
    job_info = {}
    for attr in ["id", "fqid", "status", "name", "subjobs", "application", "backend", "comment"]:
        job_info[attr] = str(getattr(j, attr))
    job_info["backend.actualCE"] = str(j.backend.actualCE)
    job_info["subjob_statuses"] = str(j.returnSubjobStatuses())

    return job_info


def get_subjob_info(job_id: int, subjob_id: int) -> dict:
    """
    Given job_id and subjob_id, return a dict container general information about the subjob.

    :param job_id: int
    :param subjob_id: int
    :return: dict
    """

    from GangaCore.GPI import jobs
    j = jobs(int(job_id))
    sj = j.subjobs[int(subjob_id)]

    # Store subjob info in a dict
    subjob_info = {}
    for attr in ["id", "fqid", "status", "name", "application", "backend", "comment"]:
        subjob_info[attr] = str(getattr(j, attr))
    subjob_info["backend.actualCE"] = str(sj.backend.actualCE)

    return subjob_info


def get_template_info(template_id: int) -> dict:
    """
    Given the template_id, return a dict containing general info of the template.

    :param template_id: int
    :return: dict
    """

    from GangaCore.GPI import templates

    t = templates[int(template_id)]

    # Store template info in a dict
    template_data = {}
    for attr in ["id", "fqid", "status", "name", "subjobs", "application", "backend", "comment"]:
        template_data[attr] = str(getattr(t, attr))

    return template_data


# ******************** Shutdown Function ******************** #

# Route used to shutdown the flask server
@app.route("/shutdown", methods=["POST"])
def shutdown():
    func = request.environ.get("werkzeug.server.shutdown")
    func()
    response_data = {"success": True, "message": "Shutting down the server..."}
    return jsonify(response_data)

# ******************** EOF ******************** #
