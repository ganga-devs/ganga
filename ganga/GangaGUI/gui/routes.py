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


# ******************** Credential Store API ******************** #


# Credential Store API - GET Method - Get list of all credentials
@app.route("/credential_store", methods=["GET"])
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
@app.route("/credential_store/renew", methods=["PUT"])
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


# ******************** Shutdown Function ******************** #

# Route used to shutdown the flask server
@app.route("/shutdown", methods=["POST"])
def shutdown():
    func = request.environ.get("werkzeug.server.shutdown")
    func()
    response_data = {"success": True, "message": "Shutting down the server..."}
    return jsonify(response_data)

# ******************** EOF ******************** #
