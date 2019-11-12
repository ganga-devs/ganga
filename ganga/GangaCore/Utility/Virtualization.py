try:
    from urllib.request import urlopen
except:
    from urllib2 import urlopen

import subprocess
import os
import urllib
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


def checkUDocker(location='~'):
    """Check whether UDocker is installed and the current user has right to access

        Return value: True or False"""

    fname = os.path.join(os.path.expanduser(location),"udocker")
    nullOutput = open(os.devnull, 'wb')
    if (os.path.isfile(fname)):
        returnCode = subprocess.call([fname, "ps"], stdout=nullOutput, stderr=nullOutput)
        if (returnCode == 0):
            return True
    return False


def installUdocker(location='~'):
    """Download and install UDocker

        Return value: True (If Success) or False"""

    fname = os.path.join(os.path.expanduser(location),"udocker")
    url = "https://raw.githubusercontent.com/indigo-dc/udocker/master/udocker.py"

    with urlopen(url) as response, open(fname, 'wb') as out_file:
        data = response.read() # a `bytes` object
        out_file.write(data)

    subprocess.call(["chmod", "u+rx", fname])
    returnCode = subprocess.call([fname, "install"])
    if (returnCode != 0):
        raise OSError('Error installing uDocker')
    os.environ['UDOCKER_DIR']=os.path.join(location,'.udocker')
    print('UDocker Successfully installed')

