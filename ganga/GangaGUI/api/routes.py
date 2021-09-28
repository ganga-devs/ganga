import json
import io
from itertools import chain
from time import time
from flask import jsonify, request, make_response
from GangaGUI.api import internal


# ******************** Internal Job API Routes ******************** #

# Single job information
@internal.route("/internal/jobs/<int:job_id>", methods=["GET"])
def job_information(job_id: int):
    """
    Gets job information from the Ganga, and returns it in JSON format.
    :param job_id: int
    :return: json
    """

    try:
        # Get the general info of the job
        job_info = get_job_info(job_id)
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(job_info)


# Single job attribute information
@internal.route("/internal/jobs/<int:job_id>/<attribute>", methods=["GET"])
def job_attribute(job_id: int, attribute: str):
    """
    returns information about the job attribute in JSON format.
    :param job_id: int
    :param attribute: str
    :return: json
    """

    from GangaCore.GPI import jobs

    try:
        # Get job attribute information
        j = jobs(job_id)
        job_attribute_info = {attribute: str(getattr(j, attribute))}

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(job_attribute_info)


# Full print of the job
@internal.route("/internal/jobs/<int:job_id>/full-print", methods=["GET"])
def job_full_print(job_id: int):
    """
    Return full print of the job.
    :param job_id: int
    :return: json
    """

    from GangaCore.GPI import jobs, full_print

    try:
        # Store full print of the job in a file like object
        print_output = io.StringIO()
        full_print(jobs(job_id), out=print_output)
        full_print_info = print_output.getvalue()

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(full_print_info)


# Create job using template
@internal.route("/internal/jobs/create", methods=["POST"])
def job_create():
    """
    Create job using the template information provided as json in the request body.
    :return: json
    """

    from GangaCore.GPI import templates, Job

    # Parse request body
    request_json = request.json if request.json else {}
    template_id = request_json.get("template_id")
    job_name = request_json.get("job_name")

    try:
        # Create job using template
        j = Job(templates[int(template_id)])
        if job_name is not None:
            j.name = job_name

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True,
                    "message": f"Job with ID {j.id} created successfully using the template with ID {template_id}"})


# Copy job
@internal.route("/internal/jobs/<int:job_id>/copy", methods=["PUT"])
def job_copy(job_id: int):
    """
    Create a copy the the job.
    :param job_id: int
    :return: json
    """

    from GangaCore.GPI import jobs

    try:
        # Copy job
        j = jobs(job_id)
        new_j = j.copy()

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": f"Successfully created a new copy with ID {new_j.id}"})


# Job action
@internal.route("/internal/jobs/<int:job_id>/<action>", methods=["PUT"])
def job_action(job_id: int, action: str):
    """
    Perform action on the job using the arguments provided as json in the request body.
    :param job_id: int
    :param action: str
    :return: json
    """

    from GangaCore.GPI import jobs
    from GangaCore.GPIDev.Lib.Job import Job

    # Store request data in a dictionary
    request_data = request.json if request.json else {}

    # Action validity check
    if action not in chain(Job._exportmethods, Job._schema.allItemNames()):
        return jsonify({"success": False, "message": f"{action} not supported or does not exist"}), 400

    # Action on Job Methods
    if action in Job._exportmethods:
        try:

            # Get job to perform action on
            j = jobs(job_id)

            # Check for arguments in the request body for passing in the method
            if action in request_data.keys():
                args = request_data[action]
                if isinstance(args, type([])):
                    getattr(j, action)(*args)
                elif isinstance(args, type({})):
                    getattr(j, action)(**args)
                else:
                    getattr(j, action)(args)
            else:
                getattr(j, action)()

        except Exception as err:
            return jsonify({"success": False, "message": str(err)}), 400

    # Action on Job Attribute
    if action in Job._schema.allItemNames():
        try:

            # Get job to perform action on
            j = jobs(job_id)

            # Check for the value to set in the request body
            if action in request_data.keys():
                arg = request_data[action]
                setattr(j, action, arg)
            else:
                return jsonify(
                    {"success": False, "message": f"Please provide the value for {action} in the request body"}), 400

        except Exception as err:
            return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(
        {"success": True, "message": f"Successfully completed the action {action} on the Job with ID {job_id}"})


# Delete job
@internal.route("/internal/jobs/<int:job_id>", methods=["DELETE"])
def job_delete(job_id: int):
    """
    Delete job from the repository.
    :param job_id: int
    :return: json
    """

    from GangaCore.GPI import jobs

    try:
        # Remove job
        j = jobs(job_id)
        j.remove()

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Job with ID {} removed successfully".format(job_id)})


# Export the job
@internal.route("/internal/jobs/<int:job_id>/export", methods=["GET"])
def job_export(job_id: int):
    """
    Exports the job to the path provided in the request argument.
    :param job_id: int
    :return: json
    """

    from GangaCore.GPI import jobs, export

    # Export save path
    save_path = request.args.get("path")

    try:
        # Export job
        export(jobs(job_id), save_path)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Successfully exported the job."})


# ******************** Internal Subjobs API Routes ******************** #

# Get jobs information
@internal.route("/internal/jobs/<int:job_id>/subjobs", methods=["GET"])
def subjobs_information(job_id: int):
    """
    Return a list of subjobs information in JSON format according to select filter and range filter. Read GUI API docstring for more information.
    :param job_id: int
    :return: json
    """

    from GangaCore.GPI import jobs

    # Subjob filter
    subjobs_filter = {
        "status": request.args.get("status"),
        "application": request.args.get("application"),
        "backend": request.args.get("backend")
    }

    # Range filter
    range_filter = {
        "recent": request.args.get("recent"),
        "length": request.args.get("length"),
        "offset": request.args.get("offset") if request.args.get("offset") is not None else 0,
    }

    # To store subjobs information
    subjobs_info = []

    # Selective subjobs info
    if "ids" in request.args:

        subjob_ids = json.loads(request.args["ids"])

        try:
            # Store specific subjobs information
            for sjid in subjob_ids:
                subjobs_info.append(get_subjob_info(job_id=job_id, subjob_id=sjid))

            return jsonify(subjobs_info)

        except Exception as err:
            return jsonify({"success": False, "message": str(err)}), 400

    # All subjobs info
    try:
        # Filter subjobs
        subjobs_filtered = list(
            jobs[job_id].subjobs.select(**{key: subjobs_filter[key] for key in subjobs_filter.keys() if
                                           subjobs_filter[key] not in ["any", None]}))

        # Reverse if recent is present in params
        if range_filter["recent"] is not None:
            subjobs_filtered.reverse()

        # Apply range filters
        if range_filter["length"] is None:
            subjobs_slice = slice(None, None)
        else:
            start = int(range_filter["offset"]) * int(range_filter["length"])
            end = start + int(range_filter["length"])
            subjobs_slice = slice(start, end)

        # Store subjobs information
        for sj in subjobs_filtered[subjobs_slice]:
            subjobs_info.append(get_subjob_info(job_id=job_id, subjob_id=sj.id))

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(subjobs_info)


# Get subjobs length
@internal.route("/internal/jobs/<int:job_id>/subjobs/length", methods=["GET"])
def subjobs_length(job_id: int):
    """
    returns total number of subjobs of a job after applying the filter.
    :param job_id: int
    :return: json
    """

    from GangaCore.GPI import jobs

    subjobs_filter = {
        "status": request.args.get("status"),
        "application": request.args.get("application"),
        "backend": request.args.get("backend")
    }

    try:
        # Get subjobs length
        subjobs_len = len(jobs[job_id].subjobs.select(**{key: subjobs_filter[key] for key in subjobs_filter.keys() if
                                                         subjobs_filter[key] not in ["any", None]}))
    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(subjobs_len)


# Single subjob infomation
@internal.route("/internal/jobs/<int:job_id>/subjobs/<int:subjob_id>", methods=["GET"])
def subjob_information(job_id: int, subjob_id: int):
    """
    Return general information about a subjob.
    :param job_id: int
    :param subjob_id: int
    """

    try:
        # Get the general info of the job
        subjob_info = get_subjob_info(job_id=job_id, subjob_id=subjob_id)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(subjob_info)


# Subjob attribute
@internal.route("/internal/jobs/<int:job_id>/subjobs/<int:subjob_id>/<attribute>", methods=["GET"])
def subjob_attribute(job_id: int, subjob_id: int, attribute: str):
    """
    Returns information about subjob attribute in JSON format.
    :param job_id: int
    :param subjob_id: int
    :param attribute: str
    """

    from GangaCore.GPI import jobs

    try:
        # Get subjob attribute info
        j = jobs[int(job_id)]
        sj = j.subjobs[int(subjob_id)]
        subjob_attribute_info = {attribute: str(getattr(sj, attribute))}

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(subjob_attribute_info)


# Subjob copy
@internal.route("/internal/jobs/<int:job_id>/subjobs/<int:subjob_id>/copy", methods=["PUT"])
def subjob_copy(job_id: int, subjob_id: int):
    """
    Create a copy of subjob to a new job.
    :param job_id: int
    :param subjob_id: int
    """

    from GangaCore.GPI import jobs

    try:
        # Copy subjob
        j = jobs[job_id]
        sj = j.subjobs[subjob_id]
        new_j = sj.copy()

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": f"Successfully created a new copy with ID {new_j.id}"})


# Subjob full print
@internal.route("/internal/jobs/<int:job_id>/subjobs/<int:subjob_id>/full-print", methods=["GET"])
def subjob_full_print(job_id: int, subjob_id: int):
    """
    Returns full print information of the subjob.
    :param job_id: int
    :param subjob_id: int
    """

    from GangaCore.GPI import jobs, full_print

    try:
        # A file like object to store full print information
        print_output = io.StringIO()
        full_print(jobs(job_id).subjobs(subjob_id), out=print_output)
        full_print_info = print_output.getvalue()

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(full_print_info)


# ******************** Internal Jobs API Routes ******************** #

# Get jobs information
@internal.route("/internal/jobs", methods=["GET"])
def jobs_information():
    """
    Return a list of jobs information in JSON format according to select filter and range filter. Read GUI API docstring for more information.
    """

    from GangaCore.GPI import jobs

    # Job select filter
    jobs_filter = {
        "status": request.args.get("status"),
        "application": request.args.get("application"),
        "backend": request.args.get("backend")
    }

    # Range filter
    range_filter = {
        "recent": request.args.get("recent"),
        "length": request.args.get("length"),
        "offset": request.args.get("offset") if request.args.get("offset") is not None else 0,
    }

    # Auto validate ids provided in the ids parameter. if not None, it will skip the non existent ids
    auto_validate_ids = request.args.get("auto-validate-ids")

    # Store jobs information
    jobs_info = []

    # Selective jobs info
    if "ids" in request.args:
        request_ids = json.loads(request.args["ids"])
        job_ids = request_ids if auto_validate_ids is None else [jid for jid in request_ids if jid in jobs.ids()]
        try:
            for jid in job_ids:
                jobs_info.append(get_job_info(jid))
            return jsonify(jobs_info)
        except Exception as err:
            return jsonify({"success": False, "message": str(err)}), 400

    # All jobs info
    try:
        # Filter jobs according to the jobs filter
        jobs_filtered = list(jobs.select(**{key: jobs_filter[key] for key in jobs_filter.keys() if
                                            jobs_filter[key] not in ["any", None]}))

        # Reverse if list if recent parameter in url is present
        if range_filter["recent"] is not None:
            jobs_filtered.reverse()

        # Jobs range filter
        if range_filter["length"] is None:
            jobs_slice = slice(None, None)
        else:
            start = int(range_filter["offset"]) * int(range_filter["length"])
            end = start + int(range_filter["length"])
            jobs_slice = slice(start, end)

        # Add job information in the list
        for j in jobs_filtered[jobs_slice]:
            jobs_info.append(get_job_info(j.id))

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(jobs_info)


# Get jobs length
@internal.route("/internal/jobs/length", methods=["GET"])
def jobs_length():
    """
    Return the number of jobs after applying the select filter.
    :return: json
    """

    from GangaCore.GPI import jobs

    # Jobs filter
    jobs_filter = {
        "status": request.args.get("status"),
        "application": request.args.get("application"),
        "backend": request.args.get("backend")
    }

    try:
        # Number of jobs in the filter
        jobs_len = len(jobs.select(**{key: jobs_filter[key] for key in jobs_filter.keys() if
                                      jobs_filter[key] not in ["any", None]}))

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(jobs_len)


# Jobs statistics
@internal.route("/internal/jobs/statistics", methods=["GET"])
def jobs_statistics():
    from GangaCore.GPI import jobs

    statistics = {}

    try:
        # Store statistics
        for stat in ["new", "running", "completed", "failed", "killed"]:
            statistics[stat] = len(jobs.select(status=stat))
            if stat == "completed":
                statistics[stat] += len(jobs.select(status="completed_frozen"))
            elif stat == "failed":
                statistics[stat] += len(jobs.select(status="failed_frozen"))

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(statistics)

# Queue Information
@internal.route("/internal/queue", methods=["GET"])
def queue_api():

    from GangaCore.Core.GangaThread.WorkerThreads import _global_queues as queues

    queues_info=[]
    com="idle"
    for elem in queues._user_threadpool.get_queue():
        user_queue={}
        user_queue["user_threadpool"]=str(elem).strip("'<>() ").replace('\'', '\"')
        queues_info.append(user_queue)

    for elem in queues._monitoring_threadpool.get_queue():
        monitor_queue={}
        monitor_queue["monitoring_threadpool"]=str(elem).strip("'<>() ").replace('\'', '\"')
        queues_info.append(monitor_queue)

    for u, m in zip(queues._user_threadpool.worker_status(), queues._monitoring_threadpool.worker_status()):
        queue_info={}
        queue_info["name_user"]=str(u[0]).strip("'<>() ").replace('\'', '\"')
        queue_info["name_monitor"]=str(m[0]).strip("'<>() ").replace('\'', '\"')

        if u[1] is None:
            queue_info["user_condition"]=str(com).strip("'<>() ").replace('\'', '\"')
        elif u[1] is not None:
            queue_info["user_condition"]=str(u[1]).strip("'<>() ").replace('\'', '\"')
        if m[1] is None:
            queue_info["monitor_condition"]=str(com).strip("'<>() ").replace('\'', '\"')
        elif m[1] is not None:
            queue_info["monitor_condition"]=str(m[1]).strip("'<>() ").replace('\'', '\"')
        
        queue_info["user_timeout"]=str(u[2]).strip("'<>() ").replace('\'', '\"')
        queue_info["monitor_timeout"]=str(m[2]).strip("'<>() ").replace('\'', '\"')
        queues_info.append(queue_info)
        
    return jsonify(queues_info)

@internal.route("/internal/queue/data", methods=["GET", "POST"])
def queue_chart_api():

    from GangaCore.Core.GangaThread.WorkerThreads import _global_queues as queues
    cdata = []
    com="idle"
    for u, m in zip(queues._user_threadpool.worker_status(), queues._monitoring_threadpool.worker_status()):
        c_info={}
        if u[1] is None:
            c_info["user_condition"]=str(com).strip("'<>() ").replace('\'', '\"')
        elif u[1] is not None:
            c_info["user_condition"]=str(u[1]).strip("'<>() ").replace('\'', '\"')
        cdata.append(c_info)
        

    count=0
    for i in range(len(cdata)):
        flag=cdata[0]
        if flag["user_condition"] == com:
            count+=1

    dat = [time() * 1000, (len(cdata)-count)]
    response = make_response(json.dumps(dat))
    response.content_type = 'application/json'
    return response


# Incomplete jobs ids
@internal.route("/internal/jobs/incomplete-ids", methods=["GET"])
def jobs_incomplete_ids():
    from GangaCore.GPI import jobs

    try:
        # Incomplete IDs list
        incomplete_ids_list = list(jobs.incomplete_ids())

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(incomplete_ids_list)


# Recent jobs
@internal.route("/internal/jobs/recent", methods=["GET"])
def jobs_recent():
    """
    Returns information about 10 recent jobs in JSON format
    :return: json
    """

    from GangaCore.GPI import jobs

    recent_jobs_info = []

    try:
        # Store information of 10 recent jobs
        recent_jobs = jobs[-10:]
        for j in recent_jobs:
            recent_jobs_info.append(get_job_info(j.id))

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(recent_jobs_info)


# ******************** Internal Config API Routes ******************** #

# Config information
@internal.route("/internal/config", methods=["GET"], defaults={"section": ""})
@internal.route("/internal/config/<section>", methods=["GET"])
def config_information(section: str):
    """
    Returns config information like section, section docstring, options, options docstring and their effective value in JSON format.
    :param section: str
    :return: json
    """

    from GangaCore.GPI import config
    from GangaCore.Utility.Config import getConfig

    # To store info of config
    config_info = []

    # List of config sections
    sections_list = config

    if section != "" and section in config:
        sections_list = [section]

    # Get each section information and append to the list
    for section in sections_list:

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
        config_info.append({
            "name": str(config_section.name),
            "docstring": str(config_section.docstring),
            "options": options_list,
        })

    return jsonify(config_info)


# ******************** Internal Templates API Routes ******************** #

# Get templates information
@internal.route("/internal/templates", methods=["GET"])
def templates_information():
    """
    Return a list of templates information in JSON format according to select filter and range filter. Read GUI API docstring for more information.
    :return: json
    """

    from GangaCore.GPI import templates

    # Templates select filter
    templates_filter = {
        "application": request.args.get("application"),
        "backend": request.args.get("backend")
    }

    # Range filter
    range_filter = {
        "recent": request.args.get("recent"),
        "length": request.args.get("length"),
        "offset": request.args.get("offset") if request.args.get("offset") is not None else 0,
    }

    templates_info = []

    try:
        # Filtered templates according to templates select filter
        templates_filtered = list(templates.select(**{key: templates_filter[key] for key in templates_filter.keys() if
                                                      templates_filter[key] not in ["any", None]}))

        # Reverse if recent parameter is present
        if range_filter["recent"] is not None:
            templates_filtered.reverse()

        # Templates range filter
        if range_filter["length"] is None:
            templates_slice = slice(None, None)
        else:
            start = int(range_filter["offset"]) * int(range_filter["length"])
            end = start + int(range_filter["length"])
            templates_slice = slice(start, end)

        # Add individual templates information to the list
        for t in templates_filtered[templates_slice]:
            templates_info.append(get_template_info(t.id))

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(templates_info)


# Full print of the template
@internal.route("/internal/templates/<int:template_id>/full-print", methods=["GET"])
def template_full_print(template_id: int):
    """
    Return full print of the template.
    :param template_id: int
    :return: json
    """

    from GangaCore.GPI import templates, full_print

    try:
        # Store full print of the template in a file like object
        print_output = io.StringIO()
        full_print(templates(template_id), out=print_output)
        full_print_info = print_output.getvalue()

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(full_print_info)


# Templates length
@internal.route("/internal/templates/length", methods=["GET"])
def templates_length():
    """
    Returns length of templates after applying the select filter
    """

    from GangaCore.GPI import templates

    # Templates filter
    templates_filter = {
        "application": request.args.get("application"),
        "backend": request.args.get("backend")
    }

    try:
        # Length of templates using the templates select filter
        templates_len = len(templates.select(**{key: templates_filter[key] for key in templates_filter.keys() if
                                                templates_filter[key] not in ["any", None]}))

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(templates_len)


# Template delete
@internal.route("/internal/templates/<int:template_id>", methods=["DELETE"])
def template_delete(template_id: int):
    """
    Delete template from the template repository.
    :param template_id: int
    """

    from GangaCore.GPI import templates

    try:
        # Remove template
        t = templates[template_id]
        t.remove()

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Template with ID {} removed successfully".format(template_id)})


# ******************** Internal Credentials API Routes ******************** #

# Credentials information
@internal.route("/internal/credentials", methods=["GET"])
def credentials_information():
    """
    Return credentials information in JSON format.
    :return: json
    """

    from GangaCore.GPI import credential_store

    # Store credential store info in a list
    credentials_info = []

    try:
        for c in credential_store:
            credential_info = {}
            credential_info["location"] = str(c.location)
            credential_info["time_left"] = str(c.time_left())
            credential_info["expiry_time"] = str(c.expiry_time())
            credential_info["is_valid"] = str(c.is_valid())
            credential_info["exists"] = str(c.exists())
            credentials_info.append(credential_info)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(credentials_info)


# Renew credentials
@internal.route("/internal/credentials/renew", methods=["PUT"])
def credentials_renew():
    """
    Renew credentials
    :return: json
    """

    from GangaCore.GPI import credential_store

    try:
        # Renew credentials
        credential_store.renew()

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Credentials successfully renewed"})


# ******************** Job Tree API ******************** #

# Job tree
@internal.route("/internal/jobtree", methods=["GET"])
def jobtree_information():
    """
    Return the job tree folder structure as the json format of python dict.
    """

    from GangaCore.GPI import jobtree

    try:
        # Reset job tree to root of job repository
        jobtree.cd()

        # Return the jobtree folder structure
        return jsonify(jobtree.folders)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400


# ******************** Internal Plugins API Routes ******************** #

# Get plugins information
@internal.route("/internal/plugins", methods=["GET"])
def plugins_information():
    """
    Returns information of plugins catergory and plugins in JSON format.
    :return: json
    """

    from GangaCore.GPI import plugins

    try:
        # Get plugins information
        plugins_info = plugins()

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(plugins_info)


@internal.route("/internal/plugins/<plugin_name>", methods=["GET"])
def plugin_information(plugin_name: str):
    """
    Returns specific information about the plugin like name and docstring

    :param plugin_name: str
    :return: json
    """

    from GangaCore import GPI

    try:
        # Get plugin
        plugin = getattr(GPI, plugin_name)
        plugin_info = {
            "name": plugin_name,
            "docstring": plugin.__doc__
        }

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(plugin_info)


# ******************** Internal Other API Routes ******************** #

# Job actions list
@internal.route("/internal/jobs/actions", methods=["GET"])
def actions():
    """
    Returns information about the attribute and methods available for the Job object.
    :return: json
    """

    from GangaCore.GPIDev.Lib.Job import Job

    try:
        # Get attribute and method information from the scheme of the Job class
        actions = {
            "attributes": Job._schema.allItemNames(),
            "methods": Job._exportmethods
        }

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify(actions)


# Load file using the load function from GPI
@internal.route("/internal/load", methods=["GET"])
def load_file():
    """
    Load the ganga object from the file path provided in the parameter.
    NOTE: Need to provide 'path' as a parameter to the request
    :return: json
    """

    from GangaCore.GPI import load

    # Path from the request argument
    path = request.args.get("path")

    try:
        # Load using the load function of ganga GPI
        load(path)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Successfully loaded the file."})


# Run file using the runfile function from GPI
@internal.route("/internal/runfile", methods=["GET"])
def run_file():
    """
    Run the file from the path provided in the request argument.
    Note: you need to provide the 'path' of the file to the request as parameter.
    :return:
    """

    from GangaCore.GPI import runfile

    # Get file path from the request argument
    path = request.args.get("path")

    try:
        # Run file using the runfile funcition on ganga GPI
        runfile(path)

    except Exception as err:
        return jsonify({"success": False, "message": str(err)}), 400

    return jsonify({"success": True, "message": "Successfully ran the file."})


# Ping route to check if server is online
@internal.route("/ping")
def ping():
    return jsonify(True)


# ******************** Helper Functions ******************** #

def get_job_info(job_id: int) -> dict:
    """
    Given the job_id, return a dict containing
    [id, fqid, status, name, subjobs, application, backend, backend.actualCE, comments, subjob_statuses, outputdir, inputdir] info of the job.

    :param job_id: int
    :return: dict
    """

    from GangaCore.GPI import jobs

    j = jobs[int(job_id)]

    # Store job info in a dict
    job_info = {}
    for attr in ["id", "fqid", "status", "name", "subjobs", "application", "backend", "comment", "outputdir",
                 "inputdir"]:
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

    from GangaCore.GPI import jobs

    j = jobs(int(job_id))
    sj = j.subjobs[int(subjob_id)]

    # Store subjob info in a dict
    subjob_info = {}
    for attr in ["id", "fqid", "status", "name", "application", "backend", "comment", "outputdir", "inputdir"]:
        subjob_info[attr] = str(getattr(sj, attr))
    subjob_info["backend.actualCE"] = str(sj.backend.actualCE)

    return subjob_info


def get_template_info(template_id: int) -> dict:
    """
    Given the template_id, return a dict containing general info of the template.

    :param template_id: int
    :return: dict
    """

    from GangaCore.GPI import jobs, templates

    t = templates[int(template_id)]

    # Store template info in a dict
    template_info = {}
    for attr in ["id", "fqid", "name", "application", "backend", "comment"]:
        template_info[attr] = str(getattr(t, attr))
    template_info["backend.actualCE"] = str(t.backend.actualCE)

    return template_info


# ******************** Shutdown Function ******************** #

# Route used to shutdown the Internal API flask server running on GangaThread [INTERNAL ROUTE DONT USE, USE /shutdown GET ROUTE]
@internal.route("/shutdown", methods=["POST"])
def _shutdown():
    func = request.environ.get("werkzeug.server.shutdown")
    func()
    response_data = {"success": True, "message": "Shutting down the server..."}
    return jsonify(response_data)


# Route used to shutdown the Internal API flask server and GUI server. [USE THIS]
@internal.route("/shutdown", methods=["GET"])
def shutdown():

    from GangaGUI.start import stop_gui
    stop_gui()

    return jsonify({"success": True, "message": "Shutdown Successful."})

# ******************** EOF ******************** #
