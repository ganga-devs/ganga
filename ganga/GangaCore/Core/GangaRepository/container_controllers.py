# TODO: Add progress bars, when pulling docker containers (stream logs to show progress?)

import os
import time
import sys
import docker
import subprocess
import gdown

from GangaCore.Utility import logging
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.Config.Config import ConfigError
from GangaCore.Utility.Virtualization import (
    checkDocker, checkUDocker,
    checkSingularity, installUdocker
)
from GangaCore.Utility.Config import get_unique_name, get_unique_port
from GangaCore.Utility.Decorators import repeat_while_none

logger = logging.getLogger()
UDOCKER_LOC = os.path.expanduser(getConfig("Configuration")["UDockerlocation"])


class ContainerCommandError(Exception):
    """Expection for when errors occur after running subprocess like container commands"""

    def __init__(self, message, controller):
        """
        Args:
            message (str): Error message
            command (str): Command that resulted in the error
            controller (str): Controller on which the command was run
        """
        self.message = message
        self.controller = controller

    def __str__(self):
        return f"[{self.controller}]: Errored with {self.message}"


def mongo_processes():
    """
    Lists all the running processes on the system
    """
    status_command = "ps -aux | grep mongod"
    output = subprocess.Popen(
        ['ps', 'aux'],
        stdout=subprocess.PIPE
    ).stdout.readlines()
    output = [i.decode("utf-8") for i in output]
    headers = [h for h in ' '.join(output[0].strip().split()).split() if h]
    raw_data = map(
        lambda s:
        s.strip().split(None, len(headers) - 1), output[1:]
    )
    procs = [dict(zip(headers, r)) for r in raw_data]

    # check if the program is related to mongo
    mongo_procs = [proc for proc in procs if "mongo" in proc["COMMAND"]]
    for idx in range(len(mongo_procs)):
        mongo_procs[idx]["environ"] = get_proc_environ(mongo_procs[idx]["PID"])

    return mongo_procs


def get_proc_environ(pid):
    """
    Get environ information from the proc/pid/environ
    """
    output = subprocess.Popen(
        ["cat", f"/proc/{pid}/environ"],
        stdout=subprocess.PIPE
    ).stdout.readlines()
    output = [i.decode("utf-8") for i in output]

    return " ".join(output)


def checkNative():
    """
    Check if the mongo database is instanlled locally
    """
    cmds = ["mongo", "mongod"]  # commands to check if mongo is installed
    nullOutput = open(os.devnull, 'wb')
    returnCode = 1
    try:
        for cmd in cmds:
            returnCode = subprocess.call(
                [cmd], stdout=nullOutput, stderr=nullOutput, shell=True)
    except Exception as e:
        print(e)
    if returnCode == 0:
        return True
    return False


def get_database_config(gangadir):
    """
    Generate the requried variables for database config
    The username, containerName and port are saved in the $GANGADIR/container.rc

    """
    values_to_copy = ["controller", "host",
                      "baseImage", "username", "password"]
    logger = getLogger()
    config = dict([
        (key, getConfig("DatabaseConfiguration")[key])
        for key in values_to_copy
    ])
    container_config_loc = os.path.join(getConfig("Configuration")[
                                        'gangadir'], "container.rc")

    if gangadir:
        container_config_loc = os.path.join(gangadir, "container.rc")

    try:
        testing_flag = getConfig("TestingFramework")['Flag']
    except (KeyError, ConfigError):
        testing_flag = False

    if os.path.exists(container_config_loc):
        logger.debug(
            f"Reading container things from: {container_config_loc}")
        container_name, dbname, port = open(
            container_config_loc, "r").read().split()
    else:
        logger.debug(f"Generating container.rc: {container_config_loc}")
        from GangaCore.Utility.Config import get_unique_name, get_unique_port

        if testing_flag is True:
            container_name, dbname, port = "testDatabase", "testDatabase", 27017
        else:
            temp = get_unique_name()
            container_name, dbname, port = temp, temp, get_unique_port()
            with open(container_config_loc, "w") as file:
                file.write(container_name)
                file.write("\n")
                file.write(dbname)
                file.write("\n")
                file.write(str(port))

    config.update({"containerName": container_name})
    config.update({"dbname": dbname})
    config.update({"port": port})

    return config


def mongod_exists(controller, cname=None):
    """
    Check of `mongod` process is running on the system
    Args:
        controller (str): Name of the controller that started the job
    """
    if controller not in ["udocker", "singularity"]:
        raise NotImplementedError(
            f"Not Implemented for controller of type: {controller}"
        )

    all_procs = mongo_processes()
    procs = [proc for proc in all_procs]
    for proc in procs:
        if controller == "udocker":
            if (
                cname
                and "container_names" in proc["environ"]
                and cname in proc["environ"]
            ):
                return proc
        elif controller == "singularity":
            if "SINGULARITY_CONTAINER" in proc["environ"] and cname in proc["environ"]:
                return proc
    return None


@repeat_while_none(max=10, message='Waiting for Mongo DB to start')
def mongod_exists_wait(controller, cname=None):
    return mongod_exists(controller, cname)


def check_sif_file_hash(path):
    """Check the hash of the sif file used by singularity
    Args:
        path (str): Location of the sif file
    """
    import hashlib

    DEF_HASH = "a3833a7eb669f21f27de130495e2e1ba"

    m = hashlib.md5()
    hashfile = open(path, "rb")
    while True:
        data = hashfile.read(4*1024*1024)
        if not data:
            break
        m.update(data)
    hashedMd5 = m.hexdigest()

    if DEF_HASH == hashedMd5:
        return True
    else:
        return False


def download_mongo_sif(path):
    """
    Download and save the mongo.sif file in the path
    """
    os.makedirs(path, exist_ok=True)
    url = 'https://cernbox.cern.ch/index.php/s/fv37NbLf24FpoWY/download'
    fname = os.path.join(path, 'mongo.sif')

    gdown.download(url, fname, quiet=False)


def create_mongodir(gangadir):
    """
    Will create a the required data folder for mongo db
    """
    data_path = os.path.join(gangadir, "data")
    if not os.path.exists(data_path):
        dirs_to_make = [
            data_path, os.path.join(data_path, "db"),
            os.path.join(gangadir, "logs"),
            os.path.join(data_path, "configdb")
        ]
        _ = [*map(lambda x: os.makedirs(x, exist_ok=True), dirs_to_make)]

    return os.path.join(gangadir, "data")


def native_handler(database_config, action, gangadir):
    """
    Will handle when the database is installed locally
    Assumptions:
    1. Database is already started, ganga should not explicitely start the database, as ganga may not have permissions
    2. Database cannot be shut by ganga, citing the same reasons as above.
    """
    if action not in ["start", "quit"]:
        raise NotImplementedError(f"Illegal Opertion on container")
    if action == "start":
        logger.info("Native Database detection, skipping startup")
    else:
        logger.info("Native Database detection, skipping closing")


def singularity_handler(database_config, action, gangadir):
    """
    Handle the starting and closing of singularty container for the ganga database.

    Args:
        database_config: The config from ganga config
        action: The action to be performed using the handler
    """
    sif_file = os.path.join(gangadir, "mongo.sif")

    if not os.path.isfile(sif_file):
        download_mongo_sif(gangadir)
        # raise FileNotFoundError(
        #     "The mongo.sif file does not exists. " +
        #     "Please read: https://github.com/ganga-devs/ganga/wiki/GangaDB-User-Guide",
        #     sif_file,
        # )

    bind_loc = create_mongodir(gangadir=gangadir)
    start_container = f"""env MONGO_SIF={sif_file} \
    singularity --quiet run \
    --bind {bind_loc}:/data \
    {sif_file} mongod \
    --port {database_config['port']} \
    --logpath {gangadir}/logs/mongod-ganga.log"""

    stop_container = f"""env MONGO_SIF={sif_file} \
    singularity --quiet run \
    --bind {bind_loc}:/data \
    {sif_file} mongod --port {database_config['port']} --shutdown"""

    if not checkSingularity():
        raise Exception("Singularity seems to not be installed on the system.")
    if action not in ["start", "quit"]:
        raise NotImplementedError(f"Illegal Opertion on container")

    has_correct_hash = check_sif_file_hash(sif_file)
    if not has_correct_hash:
        import sys
        logger.fatal((
            f"sif_file at {sif_file} does not match the expected hash." +
            " Delete it to automatically download the correct file."
        ))
        sys.exit(1)

    if action == "start":
        proc_status = mongod_exists(controller="singularity", cname=sif_file)
        if proc_status is None:
            logger.debug(
                f"Starting singularity container using command: {start_container}")
            proc = subprocess.Popen(
                start_container,
                shell=True,
                close_fds=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            proc_status = mongod_exists_wait(
                controller="singularity", cname=sif_file)

            if proc_status is None:
                # reading the logs from the file
                logger.fatal('Problem finding singularity process')
                try:
                    import json
                    err_string = open(
                        f"{gangadir}/logs/mongod-ganga.log", "r").read()
                    for _log in err_string.split("\n"):
                        if _log:
                            log = json.loads(_log)
                            if log['s'] == "E":
                                logger.error(
                                    f"Singularity container could not start because of: {log['attr']['error']}")
                except:
                    pass
                import sys
                sys.exit(1)
            logger.info(
                f"Singularity gangaDB started on port: {database_config['port']}"
            )
    elif action == "quit":
        proc_status = mongod_exists(controller="singularity", cname=sif_file)
        if proc_status is not None:
            logger.debug(
                f"mongod process killed was: {proc_status}")
            logger.debug("stopping singularity container using: ",
                         stop_container)
            proc = subprocess.Popen(
                stop_container,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = proc.communicate()
            if err:
                raise ContainerCommandError(
                    message=err.decode() + f"{proc_status}", controller="singularity"
                )
            logger.info("Singularity gangaDB has shutdown")


def udocker_handler(database_config, action, gangadir):
    """
    Will handle the loading of container using docker
    -------
    database_config: The config from ganga config
    action: The action to be performed using the handler
    """

    bind_loc = create_mongodir(gangadir=gangadir)
    container_loc = os.path.join(
        UDOCKER_LOC, ".udocker", "containers", database_config["containerName"]
    )
    stop_container = f"udocker rm {database_config['containerName']}"

    create_container = f"""udocker create \
    --name={database_config['containerName']} \
    {database_config['baseImage']}"""

    start_container = f"""udocker run \
    --volume={bind_loc}/db:/data/db \
    --publish={database_config['port']}:27017 \
    {database_config['containerName']} --logpath mongod-ganga.log
    """

    if not checkUDocker():
        # if checkUDocker():
        raise Exception("Udocker seems to not be installed on the system.")
    if action not in ["start", "quit"]:
        raise NotImplementedError(f"Illegal Opertion on container")

    if not os.path.exists(container_loc):
        logger.info(
            f"Creating udocker container for {database_config['baseImage']}")
        proc = subprocess.Popen(
            create_container,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, err = proc.communicate()
        if err:
            logger.debug(f"create_container commands: {create_container}")
            raise ContainerCommandError(
                message=err.decode(), controller="udocker")

    if action == "start":
        proc_status = mongod_exists(
            controller="udocker", cname=database_config["containerName"]
        )
        if proc_status is None:
            proc = subprocess.Popen(
                start_container,
                shell=True,
                close_fds=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            proc_status = mongod_exists_wait(
                controller="udocker", cname=database_config["containerName"]
            )
            if proc_status is None:
                logger.debug(f"start_container commands: {start_container}")
                import shutil

                src = os.path.join(
                    container_loc,
                    "ROOT",
                    "root",
                    "mongod-ganga.log",
                )
                dest = os.path.join(gangadir, "logs", "mongod-ganga.log")
                shutil.copy(src=src, dst=dest)

                raise ContainerCommandError(
                    message="Check the logs at $GANGADIR/logs/mongod-ganga.log",
                    controller="udocker"
                )
            # out, err = proc.communicate() # DO NOT COMMUNICATE THIS PROCESS
            logger.info(
                f"uDocker gangaDB should have started on port: {database_config['port']}"
            )
    else:
        proc_status = mongod_exists(
            controller="udocker", cname=database_config["containerName"]
        )
        if proc_status is not None:
            # if proc_status is not None or errored:
            proc = subprocess.Popen(
                stop_container,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = proc.communicate()
            if err:
                logger.debug(f"stop_container commands: {stop_container}")
                raise ContainerCommandError(
                    message=err.decode() + f"{proc_status}", controller="udocker"
                )
            logger.info("uDocker gangaDB should have shutdown")


def docker_handler(database_config, action, gangadir):
    """
    Will handle the loading of container using docker

    Args:
    database_config: The config from ganga config
    action: The action to be performed using the handler
    """
    if not checkDocker():
        raise Exception("Docker seems to not be installed on the system.")
    if action not in ["start", "quit"]:
        raise NotImplementedError(f"Illegal Opertion on container")

    container_client = docker.from_env()
    if action == "start":
        try:
            container = container_client.containers.get(
                database_config["containerName"]
            )
            if container.status != "running":
                container.restart()
                logger.info(
                    f"Docker gangaDB has started in background at {database_config['port']}"
                )
            else:
                logger.debug(
                    "Docker gangaDB was already running in background")

        except docker.errors.NotFound:
            bind_loc = create_mongodir(gangadir=gangadir)

            container = container_client.containers.run(
                detach=True,
                name=database_config["containerName"],
                image=database_config["baseImage"],
                ports={"27017/tcp": database_config["port"]},
                # volumes=[f"{bind_loc}:/data"]
                volumes=[f"{bind_loc}/db:/data/db"],
            )

            logger.info(
                f"Docker gangaDB has started in background at {database_config['port']}"
            )

            # check whether the container started
        except Exception as e:
            # TODO: Handle gracefull quiting of ganga
            logger.error(e)
            logger.info("Quiting ganga as the mongo backend could not start")
            raise e

        return True
    else:
        # killing the container
        try:
            container = container_client.containers.get(
                database_config["containerName"]
            )
            container.kill()
            # call the function to get the gangadir here
            logger.info("Docker gangaDB has been shutdown")
        except docker.errors.APIError as e:
            if e.response.status_code == 409:
                logger.debug(
                    "Docker container was already killed by another registry")
            else:
                raise e
        return True
