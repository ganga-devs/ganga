# TODO: Add progress bars, when pulling docker containers (stream logs to show progress?)

import os
import time
import docker
import psutil
import subprocess

from GangaCore.Utility import logging
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Virtualization import (
    checkDocker,
    checkUDocker,
    checkSingularity,
    installUdocker,
)

logger = logging.getLogger()

GANGADIR = os.path.expandvars("$HOME/gangadir")
DATABASE_CONFIG = getConfig("DatabaseConfigurations")
UDOCKER_LOC = os.path.expanduser(getConfig("Configuration")["UDockerlocation"])


class ContainerCommandError(Exception):
    """Expection for when errors occur after running subprocess like container commands"""

    def __init__(self, message, command, controller):
        """
        Args:
            message (str): Error message
            command (str): Command that resulted in the error
            controller (str): Controller on which the command was run
        """
        self.message = message
        self.command = command
        self.controller = controller

    def __str__(self):
        return f"[{self.controller}]: Error {self.message} while running command: {self.command}"


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

    procs = [proc for proc in psutil.process_iter() if proc.name() == "mongod"]
    for proc in procs:
        proc_dict = proc.as_dict()
        if "environ" in proc_dict and proc_dict["environ"]:
            if controller == "udocker":
                if (
                    cname
                    and "container_names" in proc_dict["environ"]
                    and proc_dict["environ"]["container_names"] == cname
                ):
                    return proc
                if ["container_uuid", "container_root", "container_names"] in list(
                    proc_dict["environ"].keys()
                ):
                    return proc
            elif controller == "singularity":
                if "SINGULARITY_NAME" in proc_dict["environ"]:
                    if proc_dict["environ"]["SINGULARITY_NAME"] == cname:
                        return proc
    return None


def create_mongodir(gangadir):
    """
    Will create a the required data folder for mongo db
    """
    dirs_to_make = [
        os.path.join(gangadir, "data"),
        os.path.join(gangadir, "data/db"),
        os.path.join(gangadir, "data/configdb"),
    ]
    _ = [*map(lambda x: os.makedirs(x, exist_ok=True), dirs_to_make)]

    return os.path.join(gangadir, "data")


def native_handler(database_config, action="start", gangadir=GANGADIR):
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


def udocker_handler(database_config, action="start", gangadir=GANGADIR, errored=False):
    """
    Will handle the loading of container using docker
    -------
    database_config: The config from ganga config
    action: The action to be performed using the handler

    start: udocker create     --name=ratin     mongo
    run : udocker run     --volume=/home/dumbmachine/gangadir/data/db:/data/db     --publish=56033:27017     ratin
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

    # check if the container exists
    if not os.path.exists(container_loc):
        # create the container
        logger.info(f"Creating udocker container for {database_config['baseImage']}")
        print(create_container)

        proc = subprocess.Popen(
            create_container,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, err = proc.communicate()
        if err:
            raise ContainerCommandError(
                message=err.decode(), controller="udocker", command=create_container
            )

    if action == "start":
        proc_status = mongod_exists(
            controller="udocker", cname=database_config["containerName"]
        )
        if proc_status is None:
            print(start_container)
            proc = subprocess.Popen(
                # start_container,
                [
                    "udocker run     --volume=/home/dumbmachine/gangadir/data/db:/data/db     --publish=56033:27017     fcaaa19e_eb4a_11ea_8ea8_278d184438c5 --logpath mongod-ganga.log"
                ],
                shell=True,
                close_fds=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(1) # give a second for the above ommand to propagate
            proc_status = mongod_exists(
                controller="udocker", cname=database_config["containerName"]
            )
            if proc_status is None:
                # copy the log file from container to gangadir
                import shutil

                src = os.path.join(
                    container_loc, "ROOT", "root",
                    "mongod-ganga.log",
                )
                dest = os.path.join(gangadir, "logs", "mongod-ganga.log")
                shutil.copy(src=src, dst=dest)

                raise ContainerCommandError(
                    message="Check the logs at $GANGADIR/logs/mongod-ganga.log", controller="udocker", command=create_container
                )
            # out, err = proc.communicate() # DO NOT COMMUNICATE THIS PROCESS
            logger.info(
                f"gangaDB should have started on port: {database_config['port']}"
            )
    else:
        proc = mongod_exists(
            controller="udocker", cname=database_config["containerName"]
        )

        if proc is not None or errored:
            proc = subprocess.Popen(
                stop_container,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = proc.communicate()
            if err:
                raise ContainerCommandError(
                    message=err.decode(), controller="udocker", command=create_container
                )
            logger.info("gangaDB should have shutdown")


def singularity_handler(database_config, action="start", gangadir=GANGADIR):
    """
    Uses spython module

    Args:
        database_config: The config from ganga config
        action: The action to be performed using the handler

    This could also help things:
    - create container: Not needed, we have the sif file
    - run the command on the container:
    - stop the container: DOCKER_COMMAND=mongo --eval 'db.getSiblingDB("admin").shutdownServer()'
    """
    from spython.main import Client

    # check if the singularity sif exists
    sif_file = os.path.join(gangadir, "mongo.sif")
    if not os.path.isfile(sif_file):
        raise FileNotFoundError(
            "The mongo.sif file does not exists. Please download it using DOWNLOAD_URL and store it: ",
            sif_file,
        )

    bind_loc = create_mongodir(gangadir=gangadir)
    container_flag_loc = os.path.join(gangadir, "container.flag")

    if not checkSingularity():
        raise Exception("Singularity seems to not be installed on the system.")
    if action not in ["start", "quit"]:
        raise NotImplementedError(f"Illegal Opertion on container")

    if action == "start":
        # check if the container is already running
        if (
            os.path.exists(container_flag_loc)
            and open(container_flag_loc, "r").read() == "True"
        ):
            return True, None  # the container is already running

        options = ["--bind", f"{bind_loc}:/data"]
        std = Client.execute(
            sif_file,
            f"mongod --port {database_config['port']} --fork --logpath /data/daemon-mongod.log",
            options=options,
        )

        if isinstance(std, dict):
            raise Exception(
                "An error occured while trying to fork proc ", std["message"]
            )
            return False, "An error occured while trying to fork proc" + std["message"]

        open(container_flag_loc, "w").write("True")

        # read the logs to see if there are any issues
        # to_raise = check_logs()
        # if to_raise:
        #     raise Exception("There was a error while forking mognod, please check the logs at ~/gangadir/data/daemon-mongod.log")

        logger.info(f"Singularity started mongodb on port: {database_config['port']}")

    elif action == "quit":
        std = Client.execute(
            sif_file,
            f"mongod --dbpath {bind_loc}/db --port {database_config['port']} --shutdown",
        )

        if isinstance(std, dict):
            raise Exception("error while stopping mongod", std["message"])

        open(container_flag_loc, "w").write("False")
        logger.info("Singularity MongoDB shutdown")
    return True, None


def docker_handler(database_config, action="start", gangadir=GANGADIR):
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
                    f"gangaDB has started in background at {database_config['port']}"
                )
            else:
                logger.debug("gangaDB was already running in background")

        except docker.errors.NotFound:
            # call the function to get the gangadir here
            bind_loc = create_mongodir(gangadir=gangadir)
            logger.info(f"Creating Container at {database_config['port']}")

            # if the container was not found by docker, lets create it.
            container = container_client.containers.run(
                detach=True,
                name=database_config["containerName"],
                image=database_config["baseImage"],
                ports={"27017/tcp": database_config["port"]},
                # volumes=[f"{bind_loc}:/data"]
                volumes=[f"{bind_loc}/db:/data/db"],
            )

            logger.info(
                f"2 gangaDB has started in background at {database_config['port']}"
            )
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
            logger.info("gangaDB has been shutdown")
        except docker.errors.APIError as e:
            if e.response.status_code == 409:
                logger.debug(
                    "database container was already killed by another registry"
                )
            else:
                raise e
        return True
