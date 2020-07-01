# ******************** Imports ******************** #

import jwt
from GangaGUI.gui import app
from flask import request, jsonify
from functools import wraps
from GangaGUI.gui.models import User


# ******************** View Routes ******************** #

# Dashboard route
@app.route("/")
def dashboard():
    return "GangaGUI"


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

# Single Job Info API - GET Method
@app.route("/job/<int:job_id>", methods=["GET"])
@token_required
def job_endpoint(current_user, job_id: int):
    """
    Given the job id returns the general information related to the job in JSON format.

    Returned job information: fqid, status, name, number of subjobs, application, backend, backend.actualCE, comments, subjobs statuses.

    :param job_id: int
    """

    # Get the general job info
    try:
        job_data = get_job_data(job_id)
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(job_data)


# Single Job Attribute Info API - GET Method
@app.route("/job/<int:job_id>/<attribute>", methods=["GET"])
@token_required
def job_attribute_endpoint(current_user, job_id: int, attribute: str):
    """
    Given the job id and attribute, return the attribute information in the string format via JSON.

    Supported attributes: application, backend, do_auto_resubmit, fqid, id, info, inputdir, inputfile, master, name, outputdir, outputfiles, parallel_submit, splitter, status, subjobs, time

    :param job_id: int
    :param attribute: str
    """

    # Imports
    from GangaCore.GPI import jobs
    from GangaCore.GPIDev.Lib.Job import Job

    # Supported attribute check
    if attribute not in Job._schema.allItemNames():
        return jsonify({"success": False,
                        "message": "Job Attribute {} is not currently supported or does not exist".format(
                            attribute)}), 400

    # Get job from jobs repository
    try:
        j = jobs[job_id]
        response_data = {attribute: str(getattr(j, attribute))}
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_data)


# Create job using existing template API - POST Method
@app.route("/job/create", methods=["POST"])
@token_required
def job_create_endpoint(current_user):
    """
    API to create a job using the existing template.

    IMPORTANT: template_id NEEDS to be provided in the request body. job_name can also be provided in the request body.
    """

    # Imports
    from GangaCore.GPI import templates, Job

    # Store request data
    template_id: int = request.form.get("template_id")
    job_name: str = request.form.get("job_name")

    # template_id existence check
    if template_id is None:
        return jsonify({"success": False,
                        "message": "Template ID not provided in the request data"}), 400

    # Create job using template
    try:
        j = Job(templates[int(template_id)])
        if job_name is not None:
            j.name = job_name
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True,
                    "message": "Job with ID {} created successfully using the template ID {}".format(j.id,
                                                                                                     template_id)})


# Perform certain action on the Job - PUT Method
@app.route("/job/<int:job_id>/<action>", methods=["PUT"])
# @token_required
def job_action_endpoint(job_id: int, action: str):
    """
    Given the job_id and action in the endpoint, perform the action on the job.

    Supported actions: do_auto_resubmit, name, parallel_submit, copy, kill, force_status, resubmit, runPostProcessors, submit

    Important Info:
    1) do_auto_resubmit: User needs to provide 'do_auto_resubmit' field and it's value in the request body.
    2) name: User needs to provide 'name' field and it's value in the request body.
    3) parallel_submit: User needs to provide 'parallel_submit' field and it's value in the request body.
    4) force_status: User needs to provide 'force_status' field and it's value in the request body.

    :param job_id: int
    :param action: str
    :param attribute: str
    """

    # Imports
    from GangaCore.GPI import jobs
    from GangaCore.GPIDev.Lib.Job import Job

    # Action support check
    if action not in Job._exportmethods:
        return jsonify({"success": False,
                        "message": "Job action {} is not currently supported or does not exist".format(action)}), 400

    request_data = request.form.to_dict()

    try:
        j = jobs(job_id)
        if action in request_data.keys():
            getattr(j, action)(request_data[action])
        else:
            getattr(j, action)()
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Successfully completed the action {} on the Job ID {}".format(action, job_id)})


# Delete Job API - DELETE Method
@app.route("/job/<int:job_id>", methods=["DELETE"])
@token_required
def job_delete_endpoint(current_user, job_id: int):
    """
    Given the job id, removes the job from the job repository.

    :param job_id: int
    """

    # Imports
    from GangaCore.GPI import jobs

    # Remove job
    try:
        j = jobs[job_id]
        j.remove()
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Job with ID {} successfully removed".format(job_id)})


# ******************** Helper Functions ******************** #

def get_job_data(job_id: int) -> dict:
    """
    Given the job_id, return a dict containing
    [id, fqid, status, name, subjobs, application, backend, backend.actualCE, comments, subjob_statuses] as dict keys and their values.

    :param job_id: int
    :return: dict
    """

    from GangaCore.GPI import jobs

    # Get job from the job list
    j = jobs[int(job_id)]

    # Store job info in a dict
    job_data = {}
    for attr in ["id", "fqid", "status", "name", "subjobs", "application", "backend", "comment"]:
        job_data[attr] = str(getattr(j, attr))
    job_data["backend.actualCE"] = str(j.backend.actualCE)
    job_data["subjob_statuses"] = str(j.returnSubjobStatuses())

    return job_data


# ******************** Shutdown Function ******************** #

# Route used to shutdown the flask server
@app.route("/shutdown", methods=["POST"])
def shutdown():
    func = request.environ.get("werkzeug.server.shutdown")
    func()
    response_data = {"success": True, "message": "Shutting down the server..."}
    return jsonify(response_data)

# ******************** EOF ******************** #
