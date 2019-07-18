import subprocess
import os


def checkSingularity():
    """Check whether Singularity is installed and the current user has right to access

        Return value: True or False"""

    nullOutput = open(os.devnull, 'wb')
    returnCode = 1
    try:
        returnCode = subprocess.call(["singularity", "--version"], stdout=nullOutput, stderr=nullOutput)
    except:
        pass 
    if returnCode == 0 : return True
    return False


def checkDocker():
    """Check whether Docker is installed and the current user has right to access

        Return value: True or False"""

    nullOutput = open(os.devnull, 'wb')
    returnCode = 1
    try:
        returnCode = subprocess.call(["docker", "ps"], stdout=nullOutput, stderr=nullOutput)
    except:
        pass 
    if returnCode == 0 : return True
    return False


def checkUDocker():
    """Check whether UDocker is installed and the current user has right to access

        Return value: True or False"""

    nullOutput = open(os.devnull, 'wb')
    if (os.path.isfile(os.path.expanduser("~") + "/udocker")):
        returnCode = subprocess.call([os.path.expanduser("~") + "/udocker", "ps"], stdout=nullOutput, stderr=nullOutput)
        if (returnCode == 0):
            return True
    return False


def installUdocker():
    """Download and install UDocker

        Return value: True (If Success) or False"""

    udocker_address = "https://raw.githubusercontent.com/indigo-dc/udocker/master/udocker.py"
    returnCode = subprocess.check_call(['curl', udocker_address], stdout=open(os.path.expanduser("~")+"/udocker", 'w'))
    if (returnCode != 0):
        print ("Error downloading UDocker")
        return False
    subprocess.call(["chmod", "u+rx", os.path.expanduser("~")+"/udocker"])
    returnCode = subprocess.call([os.path.expanduser("~")+"/udocker", "install"])
    if (returnCode != 0):
        print ("Error installing uDocker")
        return False
    print ("UDocker Successfully installed")
    return True
