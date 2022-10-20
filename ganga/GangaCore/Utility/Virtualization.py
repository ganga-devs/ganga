import subprocess
import os
import urllib
import os
import tempfile

from urllib.request import urlopen

from GangaCore.Core.exceptions import GangaIOError

def checkSingularity():
    """Check whether Singularity is installed and the current user has right to access

        Return value: True or False"""

    nullOutput = open(os.devnull, 'wb')
    returnCode = 1
    try:
        returnCode = subprocess.call(["singularity", "--version"], stdout=nullOutput, stderr=nullOutput)
    except:
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
    except:
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
    except:
        pass
    # check for local udocker
    fname = os.path.join(os.path.expanduser(location), "udocker")
    nullOutput = open(os.devnull, 'wb')
    if (os.path.isfile(fname)):
        try:
            returnCode = subprocess.call([fname, "ps"], stdout=nullOutput, stderr=nullOutput)
            if (returnCode == 0):
                return True
        except:
            pass
    return False


def installUdocker(location='~'):
    """Download and install UDocker

        Return value: True (If Success) or False"""

    location = os.path.expanduser(location)

    tarball = "udocker-1.3.4.tar.gz"
    url = "https://github.com/indigo-dc/udocker/releases/download/1.3.4/" + tarball

    import ssl
    context = ssl._create_unverified_context()

    with tempfile.TemporaryDirectory() as tmpdirname:

        fname = os.path.join(tmpdirname, tarball)

        try:
            with urlopen(url, context=context) as response, open(fname, 'wb') as out_file:
                data = response.read()
                out_file.write(data)
        except:
            returnCode = subprocess.call(['curl', '-k', url], stdout=open(fname, 'wb'))
            if (returnCode != 0):
                raise GangaIOError('Error downloading uDocker')

        returnCode = subprocess.call(["tar", "-C", location, "-xzf", fname])
        if (returnCode != 0):
            raise GangaIOError(f'Fail to unpack tarball for uDocker installation. Maybe {url} not available.')

        udockerdir = os.path.join(location, '.udocker')
        os.environ['UDOCKER_DIR'] = udockerdir
        os.makedirs(udockerdir, exist_ok=True)
        try:
            returnCode = subprocess.call([os.path.join(location, "udocker", "udocker"), "install"])
            if (returnCode != 0):
                raise GangaIOError('Error installing uDocker')
        except FileNotFoundError as e:
            raise GangaIOError(f'Error installing uDocker: {e}')

    os.makedirs(os.path.join(location, 'udocker'), exist_ok=True)
    with open(os.path.join(location, 'udocker', 'udocker.conf'), 'w') as fconfig:
        fconfig.write('http_insecure = True')
    print('UDocker Successfully installed')
