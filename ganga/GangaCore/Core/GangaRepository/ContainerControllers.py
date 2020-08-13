import os
import docker

from GangaCore.Utility import logging
from GangaCore.Utility.Config import getConfig

logger = logging.getLogger()


def native_handler(database_config, action="start"):
    """
    Will handle when the database is installed locally

    Assumptions:
    1. Database is already started, ganga should not explicitely start the database, as ganga may not have permissions
    2. Database cannot be shut by ganga, citing the same reasons as above.
    """
    if action not in ["start", "kill"]:
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
    raise NotImplementedError


def docker_handler(database_config, action="start"):
    """
    Will handle the loading of container using docker
    -------
    database_config: The config from ganga config
    action: The action to be performed using the handler
    """
    if action not in ["start", "kill"]:
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
            host_path = os.path.expanduser("~/gangadir/gangaDB")
            logger.info(
                f"Pulling a copy of baseImage: {database_config['baseImage']}")

            # if the container was not found by docker, lets create it.
            container = container_client.containers.run(
                detach=True,
                name=database_config["containerName"],
                image=database_config["baseImage"],
                ports={"27017/tcp": database_config["port"]},
                mounts=[
                    docker.types.Mount(
                        target="/data/db", source=host_path,  type="bind")
                ]
                # volumes={
                #     os.path.expanduser("~/gangadir/gangaDB"): {"bind": "/data/db", "mode": "rw"}}
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