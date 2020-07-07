import jwt
import json
from functools import wraps
from itertools import chain
from flask import request, jsonify
from GangaGUI.gui import app
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
# @token_required
# current_user,
def job_action_endpoint(job_id: int, action: str):
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


# ******************** Shutdown Function ******************** #

# Route used to shutdown the flask server
@app.route("/shutdown", methods=["POST"])
def shutdown():
    func = request.environ.get("werkzeug.server.shutdown")
    func()
    response_data = {"success": True, "message": "Shutting down the server..."}
    return jsonify(response_data)

# ******************** EOF ******************** #
