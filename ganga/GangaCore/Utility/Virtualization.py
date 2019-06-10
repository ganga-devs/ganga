import subprocess
import os

def checkSingularity():
    returnCode = 1
    try:
        returnCode = subprocess.call(["singularity", "--version"])
    except:
        pass 
    if returnCode == 0 : return True
    return False
    
def checkDocker():
    returnCode = 1
    try:
        returnCode = subprocess.call(["docker", "ps"])
    except:
        pass 
    if returnCode == 0 : return True
    return False
    
def checkUDocker():
    if (os.path.isfile(os.path.expanduser("~") + "/udocker")):
        returnCode = subprocess.call([os.path.expanduser("~") + "/udocker", "ps"])
        if (returnCode == 0):
            return True
    return False

def installUdocker():
    udocker_address = "https://raw.githubusercontent.com/indigo-dc/udocker/master/udocker.py"
    returnCode = subprocess.check_call(['curl', udocker_address], stdout=open(os.path.expanduser("~")+"/udocker", 'w'))
    if (returnCode < 0):
        print ("Error downloading UDocker")
        return
    subprocess.call(["chmod", "u+rx", os.path.expanduser("~")+"/udocker"])
    returnCode = subprocess.call([os.path.expanduser("~")+"/udocker", "install"])
    if (returnCode < 0):
        print ("Error installing uDocker")
    print ("UDocker Successfully installed")
