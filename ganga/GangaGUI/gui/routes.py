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


# ******************** Subjobs API ******************** #

# Subjobs API - GET Method
@app.route("/job/<int:job_id>/subjobs", methods=["GET"])
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
@app.route("/job/<int:job_id>/subjob/<int:subjob_id>", methods=["GET"])
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
        response_data = get_subjob_data(job_id=j.id, subjob_id=sj.id)
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_data)


# Single Subjob Attribute Info API - GET Method
@app.route("/job/<int:job_id>/subjob/<int:subjob_id>/<attribute>", methods=["GET"])
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


# ******************** Helper Functions ******************** #

def get_subjob_data(job_id: int, subjob_id: int) -> dict:
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


# ******************** Shutdown Function ******************** #

# Route used to shutdown the flask server
@app.route("/shutdown", methods=["POST"])
def shutdown():
    func = request.environ.get("werkzeug.server.shutdown")
    func()
    response_data = {"success": True, "message": "Shutting down the server..."}
    return jsonify(response_data)

# ******************** EOF ******************** #
