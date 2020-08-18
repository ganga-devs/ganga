# TODO: Add progress bars, when pulling docker containers (stream logs to show progress?)

import os
import docker
import subprocess

from GangaCore.Utility import logging
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Virtualization import checkDocker, checkUDocker, checkSingularity, installUdocker

logger = logging.getLogger()

GANGADIR = os.path.expandvars('$HOME/gangadir')
DATABASE_CONFIG = getConfig("DatabaseConfigurations")
COMMANDS = {
    "udocker": None,
    "singularity": {
        "start": {
            "register": "singularity instance start --bind {bind_loc}:/data docker://{image_name} {instance_name}",
            "exec": "singularity exec instance://{instance_name} mongod --fork --logpath /data/daemon-mongod.log"
        },
        # "start": "singularity instance start --bind data:/data {image_name} {instance_name}; singularity exec instance://ganga_mongo mongod &",
        "quit": {
            "shutdown": "singularity instance stop {instance_name}"
        }
    }
}


def create_mongodir():
    """
    Will create a the required data folder for mongo db
    """
    dirs_to_make = [os.path.join(GANGADIR, "data"),
                    os.path.join(GANGADIR, "data/db"),
                    os.path.join(GANGADIR, "data/configdb")]
    _ = [*map(lambda x: os.makedirs(x, exist_ok=True), dirs_to_make)]

    return os.path.join(GANGADIR, "data")

def native_handler(database_config, action="start"):
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


def udocker_handler(database_config, action="start"):
    """
    Will handle the loading of container using docker
    -------
    database_config: The config from ganga config
    action: The action to be performed using the handler

    Raises
    ------
    NotImplementedError
    """
    """
    udocker run -d --name db -v ~/mongo/data:/data/db -p 27017:27017 mongo:latest

    """
    run_command = f"{UDOCKER_BINARY} run {ARUMENTS} {database_config['databaseName']}"
    kwargs = {
        "d": 1,
        "-name": "mongodb",
        "v": "~/mongo/data:/data/db",
        "p": "27017:27017"
    }
    raise NotImplementedError


def singularity_handler(database_config, action="start"):
    """
    Will handle the loading of container using docker
    -------
    database_config: The config from ganga config
    action: The action to be performed using the handler
    """
    installed = checkSingularity()
    create_mongodir() # create the directories if required
    if not installed:
        raise Exception(
            "uDocker was not installed in the system. Make sure that")
    bind_loc = create_mongodir()
    for key, cmd in COMMANDS["singularity"][action].items():
        stdout = None if key == "register" else subprocess.PIPE
        command = cmd.format(
            bind_loc=bind_loc,
            instance_name=DATABASE_CONFIG["containerName"],
            image_name=DATABASE_CONFIG["baseImage"].replace(
                ":latest", "")
        )
        process = subprocess.Popen(
            command, stdout=stdout, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
    return stdout, stderr # we only need the last one


def docker_handler(database_config, action="start"):
    """
    Will handle the loading of container using docker
    -------
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
                logger.info(f"{database_config['containerName']} has started in background")
            else:
                logger.debug(f"{database_config['containerName']} was already running in background")

        except docker.errors.NotFound:
            # call the function to get the gangadir here
            bind_loc = create_mongodir()
            logger.info(
                f"Pulling a copy of baseImage: {database_config['baseImage']}")

            # if the container was not found by docker, lets create it.
            container = container_client.containers.run(
                detach=True,
                name=database_config["containerName"],
                image=database_config["baseImage"],
                ports={"27017/tcp": database_config["port"]},
                # FIXME: this causes error sometimes, when removing jobs
                # mounts=[
                #     docker.types.Mount(
                #         target="/data/db", source=host_path,  type="bind")
                # ]
                # volumes={
                #     os.path.expanduser("bind_loc"): {"bind": "/data/db", "mode": "rw"}}
            )
        except Exception as e:
            # TODO: Handle gracefull quiting of ganga
            logger.error(e)
            logger.info(
                "Quiting ganga as the mongo backend could not start")
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