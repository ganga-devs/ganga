import io
import os
import jwt
import json
from functools import wraps
from itertools import chain
from flask import request, jsonify, render_template, flash, redirect, url_for, session, send_file
from GangaGUI.gui import app
from GangaGUI.gui.models import User

# ******************** Global Variables ******************** #

# Colors showed for different job status in the GUI based on bootstrap

status_color = {
    "new": "info",
    "completed": "success",
    "failed": "danger",
    "running": "primary",
    "submitted": "secondary",
    "killed": "warning"
}

# Allowed extensions when uploading any files to GUI
ALLOWED_EXTENSIONS = {'txt', 'py'}


# ******************** View Routes ******************** #

# Dashboard view
@app.route("/")
def dashboard():
    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs

    quick_statistics = {}
    recent_jobs_info = []
    pinned_jobs = []

    try:
        # Get quick statistics
        for stat in ["new", "running", "completed", "failed", "killed"]:
            quick_statistics[stat] = len(jobs.select(status=stat))

        # Get last 10 jobs slice
        recent_jobs = list(jobs[-10:])
        for j in recent_jobs:
            recent_jobs_info.append(get_job_info(j.id))

    except Exception as err:
        # Flash the error in the GUI
        flash(str(err), "danger")

    return render_template("dashboard.html",
                           title="Dashboard",
                           quick_statistics=quick_statistics,
                           recent_jobs_info=recent_jobs_info,
                           pinned_jobs=pinned_jobs,
                           status_color=status_color)


# Config View
@app.route("/config", methods=["GET", "POST"])
def config_page():
    try:
        from GangaCore.GPI import config
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import config

    from GangaCore import getConfig

    sections = []
    config_list = []

    try:
        for c in config:
            config_list.append(c)
    except Exception as err:
        flash(str(err), "danger")

    # When GUI request for specific section
    if request.method == "POST":

        # Get section name for request form data
        section_name = request.form.get("section")

        if section_name is not None:
            section = getConfig(str(section_name))
            sections.append(section)

            return render_template("config.html", title="Config", sections=sections, configList=config_list)
        else:
            flash("Please select a config section to view.", "warning")

    # Add all sections from config
    for c in config_list:
        section = getConfig(c)
        sections.append(section)

    return render_template("config.html", title="Config", sections=sections, configList=config_list)


# Create View
@app.route("/create", methods=["GET", "POST"])
def create_page():
    try:
        from GangaCore.GPI import templates, load, runfile
    except:
        import ganga
        import ganga.ganga
        from ganga import templates, load, runfile

    # Handle file uploads
    if request.method == "POST":

        # Load from a uploaded file
        if "loadjobfile" in request.files:
            load_job_file = request.files["loadjobfile"]
            if load_job_file.filename == '':
                flash("No selected file", "warning")
                return redirect(request.url)

            # If valid file, the save the file
            if load_job_file and allowed_file(load_job_file.filename):
                save_path = os.path.join(app.config["UPLOAD_FOLDER"], "loadjobfile.txt")
                load_job_file.save(save_path)

                # Load the file
                try:
                    j = load(save_path)
                except Exception as err:
                    flash(str(err), "danger")
                    return redirect(request.url)

                flash(f"Successfully loaded the file!", "success")
                return redirect(request.url)

        # Run commands from a uploaded file
        if "runfile" in request.files:
            run_file = request.files["runfile"]
            if run_file.filename == '':
                flash('No selected file', "warning")
                return redirect(request.url)

            # If valid file, save the file
            if run_file and allowed_file(run_file.filename):
                save_path = os.path.join(app.config["UPLOAD_FOLDER"], "runfile.py")
                run_file.save(save_path)

                # Run the file
                try:
                    runfile(save_path)
                except Exception as err:
                    flash(str(err), "danger")
                    return redirect(request.url)

                flash("Successfully ran the file!", "success")
                return redirect(request.url)

        flash("No file part", "warning")
        return redirect(request.url)

    # Store templates in a list
    templates_list = []
    try:
        # Get templates
        for t in templates[-6:]:
            templates_list.append(get_template_info(t.id))
    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))

    return render_template("create.html", title="Create", templates_list=templates_list)


# Route to quick create run file - related to create view
@app.route("/create/runfile", methods=["GET", "POST"])
def quick_runfile():
    try:
        from GangaCore.GPI import runfile
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import runfile

    # Path to gui runfile
    runfile_path = os.path.join(app.config["UPLOAD_FOLDER"], "runfile.py")

    # Save runfile data
    if request.method == "POST":
        runfile_data = request.form.get("runfile-data")
        with open(runfile_path, "w+") as f:
            f.write(runfile_data)

        # Run file
        try:
            runfile(runfile_path)
            flash("Successfully ran the file!", "success")
        except Exception as err:
            flash(str(err), "danger")
            return redirect(request.url)

    return render_template("runfile.html", title="Run File")


# Templates view - list of templates in a tabular form
@app.route("/templates", methods=["GET", "POST"])
def templates_page():
    try:
        from GangaCore.GPI import plugins, templates
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import plugins, templates

    # Session defaults for remembering filter options
    if "templates_per_page" not in session:
        session["templates_per_page"] = 10
    if "templates_filter" not in session:
        session["templates_filter"] = {key: "any" for key in ["application", "backend"]}

    # Change filter values
    if request.method == "POST":
        # Add form data to session
        session["templates_per_page"] = int(request.form.get("templates-per-page"))
        session["templates_filter"] = {key: request.form.get(form_name) for key, form_name in
                                       zip(["application", "backend"], ["template-application", "template-backend"])}

    # Current page
    current_page = int(request.args.get("page") or 0)

    # Get user defined value from session
    templates_per_page = session["templates_per_page"]

    templates_info_list = []
    try:
        filtered_list = list(templates.select(
            **{key: session["templates_filter"][key] for key in session["templates_filter"].keys() if
               session["templates_filter"][key] != "any"}))
        filtered_list.reverse()

        # Calculate no of max pages
        number_of_pages = int(len(filtered_list) / templates_per_page + 1)

        # if page exceeds, redirect to last page
        if current_page >= number_of_pages:
            return redirect(url_for("templates_page", page=number_of_pages - 1))

        # templates list according to the filter
        tlist = filtered_list[
                (current_page * templates_per_page):(current_page * templates_per_page + templates_per_page)]

        # Get templates info
        for t in tlist:
            templates_info_list.append(get_template_info(t.id))

        # Get backends and applications list
        backends = plugins()["backends"]
        applications = plugins()["applications"]

    except Exception as err:
        # Flash error if any
        flash(str(err), "danger")
        return redirect(url_for("create_page"))

    return render_template("templates.html",
                           title="Templates",
                           number_of_pages=number_of_pages,
                           current_page=current_page,
                           backends=backends,
                           applications=applications,
                           templates_info_list=templates_info_list)


# Jobs view - list of jobs in a tabular form
@app.route("/jobs", methods=["GET", "POST"])
def jobs_page():
    try:
        from GangaCore.GPI import jobs, plugins
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs, plugins

    # Session defaults for remembering filter options
    if "jobs_per_page" not in session:
        session["jobs_per_page"] = 10
    if "jobs_filter" not in session:
        session["jobs_filter"] = {key: "any" for key in ["status", "application", "backend"]}

    # Change filter values
    if request.method == "POST":
        # Add form data to client session
        session["jobs_per_page"] = int(request.form.get("jobs-per-page"))
        session["jobs_filter"] = {key: request.form.get(form_name) for key, form_name in
                                  zip(["status", "application", "backend"],
                                      ["job-status", "job-application", "job-backend"])}

    # Current page
    current_page = int(request.args.get("page") or 0)

    # Get user defined value from session
    jobs_per_page = session["jobs_per_page"]

    jobs_info_list = []
    try:
        filtered_list = list(
            jobs.select(**{key: session["jobs_filter"][key] for key in session["jobs_filter"].keys() if
                           session["jobs_filter"][key] != "any"}))
        filtered_list.reverse()

        # Calculate no of max pages
        number_of_pages = int(len(filtered_list) / jobs_per_page + 1)

        # if page exceeds, redirect to last page
        if current_page >= number_of_pages:
            return redirect(url_for("jobs_page", page=number_of_pages - 1))

        # job list according to the filter
        jlist = filtered_list[(current_page * jobs_per_page):(current_page * jobs_per_page + jobs_per_page)]

        # Get job info
        for j in jlist:
            jobs_info_list.append(get_job_info(j.id))

        # Get backends and applications list
        backends = plugins()["backends"]
        applications = plugins()["applications"]

    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))

    return render_template("jobs.html",
                           title="Jobs",
                           jobs_info_list=jobs_info_list,
                           backends=backends,
                           applications=applications,
                           number_of_pages=number_of_pages,
                           current_page=current_page,
                           status_color=status_color)


# Job view - display information of the job
@app.route('/job/<int:job_id>')
def job_page(job_id: int):
    try:
        from GangaCore.GPI import jobs, full_print
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs, full_print

    from GangaCore.GPIDev.Lib.Job import Job

    # Get all attributes and methods from schema
    attrs = Job._schema.allItemNames()
    method_actions = Job._exportmethods

    # Info variables
    job_info = {}
    full_job_info = None
    stdout = None
    stderr = None

    try:
        j = jobs[int(job_id)]
        job_info = get_job_info(j.id)

        # Store full print output
        print_output = io.StringIO()
        full_print(j, out=print_output)
        full_job_info = print_output.getvalue()

        # stdout and stderr path
        stdout_path = os.path.join(j.outputdir, "stdout")
        stderr_path = os.path.join(j.outputdir, "stderr")

        # Get stdout
        if os.path.exists(stdout_path):
            with open(stdout_path) as f:
                stdout = f.read()

        # Get stderr
        if os.path.exists(stderr_path):
            with open(stderr_path) as f:
                stderr = f.read()

    except Exception as err:
        flash(str(err), "danger")

    if job_info == {}:
        return redirect(url_for("jobs_page"))

    return render_template("job.html",
                           title=f"Job {job_id}",
                           job_info=job_info,
                           status_color=status_color,
                           attrs=attrs,
                           method_actions=method_actions,
                           stdout=stdout,
                           stderr=stderr,
                           full_job_info=full_job_info)


# Export job route
@app.route("/job/<int:job_id>/export")
def export_job(job_id: int):
    try:
        from GangaCore.GPI import jobs, export
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs, export

    export_save_path = os.path.join(app.config["UPLOAD_FOLDER"], f"export.txt")

    # Expoet job
    try:
        j = jobs[job_id]
        export(j, export_save_path)
    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("job_page", job_id=job_id))

    return send_file(export_save_path, as_attachment=True, cache_timeout=0, attachment_filename=f"Job_{job_id}.txt")


# Edit job - related to Job view
@app.route("/job/<int:job_id>/edit", methods=["GET", "POST"])
def edit_job(job_id: int):
    try:
        from GangaCore.GPI import jobs, export, load
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs, export, load

    load_file_path = os.path.join(app.config["UPLOAD_FOLDER"], "loadjobfile.txt")
    export_save_path = os.path.join(app.config["UPLOAD_FOLDER"], "export.txt")

    if request.method == "POST":
        # Save the edited job info
        edited_job_info = request.form.get("edited-job-info")
        with open(load_file_path, "w+") as f:
            f.write(edited_job_info)

        # Load new job from the edited job info
        try:
            load(load_file_path)
            flash("Successfully edited job", "success")
        except Exception as err:
            flash(str(err), "danger")
            return redirect(request.url)

    exported_data = None
    try:
        j = jobs[job_id]
        export(j, export_save_path)

        # Read exported job file to display
        with open(export_save_path) as f:
            exported_data = f.read()
    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("job_page", job_id=job_id))

    return render_template("edit_job.html", title=f"Edit Job {job_id}", job_id=job_id, exported_data=exported_data)


# Browse Job directory - related to the Job view
@app.route('/job/<int:job_id>/browse', defaults={'path': ''})
@app.route("/job/<int:job_id>/browse/<path:path>")
def browse_job(job_id: int, path):
    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs

    try:
        j = jobs[job_id]
        job_base_dir = os.path.dirname(os.path.dirname(j.outputdir))
    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("job_page", job_id=job_id))

    # Joining the base and the requested path
    abs_path = os.path.join(job_base_dir, path)

    # URL path variable for going back
    back_path = os.path.dirname(abs_path).replace(job_base_dir, "")

    # Return back if path doesn't exist
    if not os.path.exists(abs_path):
        flash("Directory for this job does not exist.", "warning")
        return redirect(url_for("job_page", job_id=job_id))

    # Check if path is a file and serve
    if os.path.isfile(abs_path):
        return send_file(abs_path)

    files_info = []
    # Show directory contents
    files = os.listdir(abs_path)
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
@app.route("/job/<int:job_id>/subjobs", methods=["GET", "POST"])
def subjobs_page(job_id: int):
    try:
        from GangaCore.GPI import jobs, plugins
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs, plugins

    # Session defaults
    if "subjobs_per_page" not in session:
        session["subjobs_per_page"] = 10
    if "subjobs_filter" not in session:
        session["subjobs_filter"] = {key: "any" for key in ["status", "application", "backend"]}

    # Change filter values
    if request.method == "POST":
        # Add form data to client session
        session["subjobs_per_page"] = int(request.form.get("subjobs-per-page"))
        session["subjobs_filter"] = {key: request.form.get(form_name) for key, form_name in
                                     zip(["status", "application", "backend"],
                                         ["subjob-status", "subjob-application", "subjob-backend"])}

    # Current page
    current_page = int(request.args.get("page") or 0)

    # Get user defined value from session
    subjobs_per_page = session["subjobs_per_page"]

    subjobs_info_list = []
    try:
        j = jobs[job_id]
        filtered_list = list(
            j.subjobs.select(**{key: session["subjobs_filter"][key] for key in session["subjobs_filter"].keys() if
                                session["subjobs_filter"][key] != "any"}))
        filtered_list.reverse()

        # Calculate no of max pages
        number_of_pages = int(len(filtered_list) / subjobs_per_page + 1)

        # if page exceeds, redirect to last page
        if current_page >= number_of_pages:
            return redirect(url_for("subjobs_page", page=number_of_pages - 1, job_id=job_id))

        # subjobs list according to the filter
        sjlist = filtered_list[(current_page * subjobs_per_page):(current_page * subjobs_per_page + subjobs_per_page)]

        # Get subjob info
        for sj in sjlist:
            subjobs_info_list.append(get_subjob_info(job_id=j.id, subjob_id=sj.id))

        # Get backends and applications list
        backends = plugins()["backends"]
        applications = plugins()["applications"]

    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("job_page", job_id=job_id))

    return render_template("subjobs.html",
                           title=f"Subjobs - Job {job_id}",
                           status_color=status_color,
                           number_of_pages=number_of_pages,
                           current_page=current_page,
                           backends=backends,
                           applications=applications,
                           subjobs_info_list=subjobs_info_list,
                           job_id=job_id)


@app.route('/job/<int:job_id>/subjob/<int:subjob_id>')
def subjob_page(job_id: int, subjob_id: int):
    try:
        from GangaCore.GPI import jobs, full_print
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs, full_print

    from GangaCore.GPIDev.Lib.Job import Job

    # Get all attributes and methods from schema
    attrs = Job._schema.allItemNames()
    method_actions = Job._exportmethods

    # Info variables
    subjob_info = {}
    full_subjob_info = None
    browse_path = ""
    stdout = None
    stderr = None

    try:
        j = jobs[int(job_id)]
        sj = j.subjobs[subjob_id]
        subjob_info = get_subjob_info(job_id=j.id, subjob_id=sj.id)

        # Store full print output
        print_output = io.StringIO()
        full_print(sj, out=print_output)
        full_subjob_info = print_output.getvalue()

        j_dir_basepath = os.path.dirname(os.path.dirname(j.outputdir))
        sj_dir_basepath = os.path.dirname(os.path.dirname(sj.outputdir))
        browse_path = sj_dir_basepath.replace(j_dir_basepath, "")

        # stdout and stderr path
        stdout_path = os.path.join(sj.outputdir, "stdout")
        stderr_path = os.path.join(sj.outputdir, "stderr")

        # Get stdout
        if os.path.exists(stdout_path):
            with open(stdout_path) as f:
                stdout = f.read()

        # Get stderr
        if os.path.exists(stderr_path):
            with open(stderr_path) as f:
                stderr = f.read()

    except Exception as err:
        flash(str(err), "danger")
        return redirect(url_for("subjobs_page", job_id=job_id))

    return render_template("subjob.html",
                           title=f"Subjob {subjob_id} - Job {job_id}",
                           subjob_info=subjob_info,
                           status_color=status_color,
                           attrs=attrs,
                           method_actions=method_actions,
                           stdout=stdout,
                           stderr=stderr,
                           full_subjob_info=full_subjob_info,
                           job_id=job_id,
                           browse_path=browse_path)


# Credential Store view
@app.route('/credentials')
def credential_store_page():
    try:
        from GangaCore.GPI import credential_store
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import credential_store

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
        flash(str(err), "danger")
        return redirect(url_for("dashboard"))

    return render_template('credentials.html', credential_info_list=credential_info_list)


@app.route('/plugins')
def plugins_page():
    try:
        from GangaCore.GPI import plugins
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import plugins

    plugins_info = {}

    try:
        plugins_info = plugins()
    except Exception as err:
        flash(str(err), "danger")

    return render_template('plugins.html', plugins_info=plugins_info)


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
# @token_required
def job_attribute_endpoint(job_id: int, attribute: str):
    """
    Given the job_id and attribute, returns the attribute information in the JSON string format.

    :param job_id: int
    :param attribute: str
    :param current_user: Information of the current_user based on the request's JWT token
    """

    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs

    try:
        j = jobs[job_id]
        response_data = {attribute: str(getattr(j, attribute))}
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(response_data)


# Create Job Using Template API - POST Method
@app.route("/api/job/create", methods=["POST"])
# @token_required
def job_create_endpoint():
    """
    Create a new job using the existing template.

    IMPORTANT: template_id NEEDS to be provided in the request body. job_name can optionally be provided in the request body.

    :param current_user: Information of the current_user based on the request's JWT token
    """
    try:
        from GangaCore.GPI import templates, Job
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import templates, Job

    # Store request data
    template_id: int = request.json["template_id"]
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

    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs
    from GangaCore.GPIDev.Lib.Job import Job

    # Store request data in a dictionary
    request_data = json.loads(request.data)

    print(request_data)

    # Action validity check
    if action not in chain(Job._exportmethods, Job._schema.allItemNames()):
        return jsonify({"success": False, "message": f"{action} not supported or does not exist"}), 400

    # Action on Job Methods
    if action in Job._exportmethods:
        try:
            j = jobs(job_id)

            # Check for arguments in the request body for passing in the method
            if action in request_data.keys():
                # args = json.loads(request_data[action])
                args = request_data[action]
                print(args)
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
                # arg = json.loads(request_data[action])
                arg = request_data[action]
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
# @token_required
def subjobs_endpoint(job_id: int):
    """
    Returns a list subjobs of a particular job in a similar way as Jobs API.
    
    :param job_id: int
    :param current_user: Information of the current_user based on the request's JWT token
    """

    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs

    try:
        j = jobs(int(job_id))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    # Store subjobs information in a list
    subjobs_info_list = []

    if "ids" in request.args:
        subjob_ids = json.loads(request.args["ids"])
        try:
            for sjid in subjob_ids:
                subjobs_info_list.append(get_subjob_info(job_id=job_id, subjob_id=sjid))
            return jsonify(subjobs_info_list)
        except Exception as err:
            return jsonify({"success": False, "message": str(err)}), 400

    try:
        for sj in j.subjobs:
            subjobs_info_list.append(get_subjob_info(job_id=j.id, subjob_id=sj.id))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(subjobs_info_list)


# Single Subjob Info API - GET Method
@app.route("/api/job/<int:job_id>/subjob/<int:subjob_id>", methods=["GET"])
# @token_required
def subjob_endpoint(job_id: int, subjob_id: int):
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
# @token_required
def subjob_attribute_endpoint(job_id: int, subjob_id: int, attribute: str):
    """
    Given the job id, subjob id and attribute; return the attribute information in the string format via JSON.

    :param job_id: int
    :param subjob_id: int
    :param attribute: str
    :param current_user: Information of the current_user based on the request's JWT token
    """

    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs

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
# @token_required
def jobs_endpoint():
    """
    Returns a list of jobs with general information in JSON format.
    :param current_user: Information of the current_user based on the request's JWT token
    """

    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs

    # Store job information in a list
    job_info_list = []

    if "ids" in request.args:
        job_ids = json.loads(request.args["ids"])
        try:
            for jid in job_ids:
                job_info_list.append(get_job_info(jid))
            return jsonify(job_info_list)
        except Exception as err:
            return jsonify({"success": False, "message": str(err)}), 400

    try:
        for j in jobs:
            job_info_list.append(get_job_info(j.id))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(job_info_list)


# Refresh Dashboard Route
@app.route("/api/jobs/stats")
def jobs_stats_endpoint():
    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs

    quick_statistics = {}

    try:
        # Set quick statistics
        for stat in ["new", "running", "completed", "failed", "killed"]:
            quick_statistics[stat] = len(jobs.select(status=stat))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(quick_statistics)


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
# @token_required
def renew_credentials_endpoint():
    """
    Renew all the credentials in the credential store.
    """

    try:
        from GangaCore.GPI import credential_store
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import credential_store

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

    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs

    j = jobs[int(job_id)]

    # Store job info in a dict
    job_info = {}
    for attr in ["id", "fqid", "status", "name", "subjobs", "application", "backend", "comment"]:
        job_info[attr] = str(getattr(j, attr))
    job_info["backend.actualCE"] = str(j.backend.actualCE)
    job_info["subjob_statuses"] = str(j.returnSubjobStatuses())
    job_info["subjobs"] = str(len(j.subjobs))

    return job_info


def get_subjob_info(job_id: int, subjob_id: int) -> dict:
    """
    Given job_id and subjob_id, return a dict container general information about the subjob.

    :param job_id: int
    :param subjob_id: int
    :return: dict
    """

    try:
        from GangaCore.GPI import jobs
    except ImportError:
        import ganga
        import ganga.ganga
        from ganga import jobs

    j = jobs(int(job_id))
    sj = j.subjobs[int(subjob_id)]

    # Store subjob info in a dict
    subjob_info = {}
    for attr in ["id", "fqid", "status", "name", "application", "backend", "comment"]:
        subjob_info[attr] = str(getattr(sj, attr))
    subjob_info["backend.actualCE"] = str(sj.backend.actualCE)

    return subjob_info


def get_template_info(template_id: int) -> dict:
    """
    Given the template_id, return a dict containing general info of the template.

    :param template_id: int
    :return: dict
    """

    try:
        from GangaCore.GPI import jobs, templates
    except:
        import ganga
        import ganga.ganga
        from ganga import jobs, templates

    t = templates[int(template_id)]

    # Store template info in a dict
    template_data = {}
    for attr in ["id", "fqid", "status", "name", "subjobs", "application", "backend", "comment"]:
        template_data[attr] = str(getattr(t, attr))
    template_data["backend.actualCE"] = str(t.backend.actualCE)

    return template_data


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# ******************** Shutdown Function ******************** #

# Route used to shutdown the flask server
@app.route("/shutdown", methods=["GET"])
def shutdown():
    from GangaGUI.start import stop_gui
    stop_gui()
    return "Shutting Down.."


# Route used to shutdown the flask server [INTERNAL DONT USE TODO redirect to get if not localhost]
@app.route("/shutdown", methods=["POST"])
def _shutdown():
    func = request.environ.get("werkzeug.server.shutdown")
    func()
    response_data = {"success": True, "message": "Shutting down the server..."}
    return jsonify(response_data)

# ******************** EOF ******************** #
