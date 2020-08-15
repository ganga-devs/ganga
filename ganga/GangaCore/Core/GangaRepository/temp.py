"""
Learning to use the spython module
"""
# spython is not as good as docker-python, lets instead use `subprocesses`
import os
import subprocess
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Virtualization import checkDocker, checkUDocker, checkSingularity, installUdocker


GANGADIR = os.path.expandvars('$HOME/gangadir')
DATABASE_CONFIG = getConfig("DatabaseConfigurations")
COMMANDS = {
    "udocker": None,
    "singularity": {
        "start": {
            "register": "singularity instance start --bind {bind_loc}:/data docker://{image_name} {instance_name}",
            "exec": "singularity exec instance://{instance_name} mongod --fork --logpath /data/daemon-mongod.log"
        },
        "stop": {
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

# TODO: Add support for streaming the err from the singularity container


def start_container(service, action):
    """
    Start a `image` container from one of the supported `service`s
    """
    if service in ["udocker", "singularity"]:
        if service == "udocker":
            # installed = checkUDocker(location)
            # if not installed:
            #     raise Exception("uDocker was not installed in the system. Make sure that")
            raise NotImplementedError(
                "This feature is currently being developed.")
        else:
            installed = checkSingularity()
            if not installed:
                raise Exception(
                    "uDocker was not installed in the system. Make sure that")
            bind_loc = create_mongodir()
            for key, cmd in COMMANDS[service][action].items():
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
            return stdout, stderr

    else:
        raise NotImplementedError((f"The service {service} is not currently supported by ganga. " +
                                   f"Supported services are {['native', 'docker', 'udocker', 'singularity']}"))
