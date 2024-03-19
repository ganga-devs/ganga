import subprocess
import os
import stat
import tempfile

from GangaCore.Core.exceptions import GangaException


def checkApptainer():
    """Check whether Apptainer is installed and the current user has right to access

        Return value: True or False"""

    nullOutput = open(os.devnull, 'wb')
    returnCode = 1
    try:
        returnCode = subprocess.call(["apptainer", "--version"], stdout=nullOutput, stderr=nullOutput)
    except BaseException:
        pass
    if returnCode == 0:
        return True
    return False

# to be deprecated


def checkSingularity():
    """Check whether Singularity is installed and the current user has right to access

        Return value: True or False"""

    nullOutput = open(os.devnull, 'wb')
    returnCode = 1
    try:
        returnCode = subprocess.call(["singularity", "--version"], stdout=nullOutput, stderr=nullOutput)
    except BaseException:
        pass
    if returnCode == 0:
        return True
    return False


def checkDocker():
    """Check whether Docker is installed and the current user has right to access

        Return value: True or False"""

    nullOutput = open(os.devnull, 'wb')
    returnCode = 1
    try:
        returnCode = subprocess.call(["docker", "ps"], stdout=nullOutput, stderr=nullOutput)
    except BaseException:
        pass
    if returnCode == 0:
        return True
    return False


def checkUDocker(location='~'):
    """Check whether UDocker is installed and the current user has right to access

        Return value: True or False"""
    # check for linked udocker
    nullOutput = open(os.devnull, 'wb')
    try:
        returnCode = subprocess.call(["udocker", "--help"], stdout=nullOutput, stderr=nullOutput)
        if returnCode == 0:
            return True
    except BaseException:
        pass
    # check for local udocker
    fname = os.path.join(os.path.expanduser(location), "udocker", "bin", "udocker")
    nullOutput = open(os.devnull, 'wb')
    if (os.path.isfile(fname)):
        try:
            returnCode = subprocess.call([fname, "--help"], stdout=nullOutput, stderr=nullOutput)
            if (returnCode == 0):
                return True
        except BaseException:
            pass
    return False


def installUDocker(location='~'):
    """Download and install UDocker

        Return value: True (If Success) or False"""

    location = os.path.expanduser(location)

    installscript = f"""#!/bin/sh
    cd {location}
    python -m venv udocker
    cd udocker
    . bin/activate
    python -m pip install --upgrade pip setuptools wheel
    python -m pip install udocker
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        fname = os.path.join(tmpdirname, 'installer')
        with open(fname, 'w') as f:
            f.write(installscript)
        os.chmod(fname, stat.S_IRWXU)
        returnCode = subprocess.call([fname])
        if (returnCode != 0):
            raise GangaException('Error installing uDocker')
