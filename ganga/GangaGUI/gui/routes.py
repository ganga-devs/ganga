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


# ******************** Templates API ******************** #

# Templates API - GET Method
@app.route("/templates", methods=["GET"])
@token_required
def templates_GET_endpoint(current_user):
    """
    Returns a list of objects where each object is template data in JSON format.
    """

    # Imports
    from GangaCore.GPI import templates

    # Store templates information in a list
    templates_data_list = []
    try:
        for t in templates:
            templates_data_list.append(get_template_data(t.id))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(templates_data_list)


# Template API - DELETE Method
@app.route("/template/<int:template_id>", methods=["DELETE"])
@token_required
def template_DELETE_endpoint(current_user, template_id: int):
    """

    Given the templates id, delete it from the template repository.

    :param template_id: int
    """

    # Imports
    from GangaCore.GPI import templates

    # Remove template
    try:
        t = templates[template_id]
        t.remove()
    except Exception as err:
        return jsonify({"success": False, "message": str(err)})

    return jsonify({"success": True, "message": "Template with ID {} removed successfully".format(template_id)})


# ******************** Helper Functions ******************** #

def get_template_data(template_id: int) -> dict:
    """
    Given the template_id, return a dict containing general info of the template.

    :param template_id: int
    :return: dict
    """

    # Imports
    from GangaCore.GPI import templates

    # Get template from the templates list
    t = templates[int(template_id)]

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
