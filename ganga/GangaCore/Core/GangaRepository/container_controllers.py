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


def create_mongodir(gangadir):
    """
    Will create a the required data folder for mongo db
    """
    dirs_to_make = [os.path.join(gangadir, "data"),
                    os.path.join(gangadir, "data/db"),
                    os.path.join(gangadir, "data/configdb")]
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


def udocker_handler(database_config, action="start", gangadir=GANGADIR):
    """
    Will handle the loading of container using docker
    -------
    database_config: The config from ganga config
    action: The action to be performed using the handler

    Raises
    ------
    NotImplementedError
    """
    import subprocess

    bind_loc = create_mongodir(gangadir=gangadir)
    list_images = f"udocker ps"
    stop_container = f"udocker rm {database_config['containerName']}"
    start_container = f"udocker run  --volume={bind_loc}/db:/data/db {database_config['containerName']}"
    create_container = f"udocker create --name={database_config['containerName']} {database_config['baseImage']}"

    if not checkUDocker():
    # if checkUDocker():
        raise Exception("Udocker seems to not be installed on the system.")
    if action not in ["start", "quit"]:
        raise NotImplementedError(f"Illegal Opertion on container")

    logger.info("Would recommend not to use udocker")
    if action == "start":
        # check if the container exists already
        proc = subprocess.Popen(
            list_images, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if err:
            # TODO: Some Errors can be ignored
            raise Exception(err)

        if database_config['containerName'] not in out.decode():
            # run the container
            proc = subprocess.Popen(
                create_container, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            out, err = proc.communicate()
            if err:
                raise Exception(err)

            proc = subprocess.Popen(
                start_container, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                close_fds=True
            )
            # out, err = proc.communicate() # DO NOT COMMUNICATE THIS PROCESS
            logger.info("gangaDB should have started in background")
    else:
        # check if the container exists already
        proc = subprocess.Popen(
            list_images, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        out, err = proc.communicate()
        if err:
            # TODO: Some Errors can be ignored
            raise Exception(err)

        if "gangaDB" in out.decode():
            proc = subprocess.Popen(
                stop_container, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            # out, err = proc.communicate() DO NOT COMMUNICATE THIS PROCESS
            logger.info("gangaDB should have shutdown")


def singularity_handler(database_config, action="start", gangadir=GANGADIR):
    """
    Uses spython module

    Args:
        database_config: The config from ganga config
        action: The action to be performed using the handler
    """
    from spython.main import Client

    if not checkSingularity():
        raise Exception("Singularity seems to not be installed on the system.")
    if action not in ["start", "quit"]:
        raise NotImplementedError(f"Illegal Opertion on container")

    if action == "start":
        container_exists = any([
            instance.name == database_config["containerName"]
            for instance in Client.instances(quiet=True)
        ])
        if not container_exists:
            bind_loc = create_mongodir(gangadir=gangadir)
            options = [
                "--bind", f"{bind_loc}:/data"
            ]
            container = Client.instance(
                f"docker://{database_config['baseImage']}",
                options=options, name=database_config['containerName']
            )
            # container.start()
            Client.execute(
                container, "mongod --fork --logpath /data/daemon-mongod.log", quiet=True)
            logger.info("gangaDB has started in background")
        else:
            logger.debug("gangaDB was already running in background")
    else:
        for instance in Client.instances(quiet=True):
            if instance.name == database_config['containerName']:
                instance.stop()
                logger.info("gangaDB has been shutdown")


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
                logger.info("gangaDB has started in background")
            else:
                logger.debug("gangaDB was already running in background")

        except docker.errors.NotFound:
            # call the function to get the gangadir here
            bind_loc = create_mongodir(gangadir=gangadir)
            logger.info(
                f"Pulling a copy of baseImage: {database_config['baseImage']}")

            # if the container was not found by docker, lets create it.
            container = container_client.containers.run(
                detach=True,
                name=database_config["containerName"],
                image=database_config["baseImage"],
                ports={"27017/tcp": database_config["port"]},
                # volumes=[f"{bind_loc}:/data"]
                volumes=[f"{bind_loc}/db:/data/db"]
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