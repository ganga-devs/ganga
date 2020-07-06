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
