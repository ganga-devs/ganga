import os
import sys
import time
import socket
import inspect
import traceback
import pickle
import uuid
from GangaCore.Runtime.GPIexport import exportToGPI
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger
#from GangaCore.Core.GangaThread.WorkerThreads.WorkerThreadPool import WorkerThreadPool
#from GangaCore.Core.GangaThread.WorkerThreads.ThreadPoolQueueMonitor import ThreadPoolQueueMonitor
from GangaDirac.Lib.Utilities.DiracUtilities import execute
logger = getLogger()
#user_threadpool       = WorkerThreadPool()
#monitoring_threadpool = WorkerThreadPool()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#


def diracAPI(cmd, timeout=60, cred_req=None):
    '''
    Args:
        cmd (str): This is the command you want to execute from within an active DIRAC session
        timeout (int): This is the maximum time(sec) the session has to complete the task
        cred_req (ICredentialRequirement): This is the (optional) credential passed to construct the correct DIRAC env

    Execute DIRAC API commands from w/in GangaCore.

    The stdout will be returned, e.g.:

    # this will simply return 87
    diracAPI(\'print 87\')

    # this will return the status of job 66
    # note a Dirac() object is already provided set up as \'dirac\'
    diracAPI(\'print(Dirac().getJobStatus([66]))\')
    diracAPI(\'print(dirac.getJobStatus([66]))\')

    # or can achieve the same using command defined and included from
    # getConfig('DIRAC')['DiracCommandFiles']
    diracAPI(\'status([66])\')

    '''
    return execute(cmd, timeout=timeout, cred_req=cred_req)

exportToGPI('diracAPI', diracAPI, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#

running_dirac_process = False
dirac_process = None
dirac_process_ids = None
def startDiracProcess():
    '''
    Start a subprocess that runs the DIRAC commands
    '''
    HOST = 'localhost'  #Connect to localhost
    end_trans = '###END-TRANS###'
    import subprocess
    from GangaDirac.Lib.Utilities.DiracUtilities import getDiracEnv, getDiracCommandIncludes, GangaDiracError
    global dirac_process
    #Some magic to locate the python script to run
    from GangaDirac.Lib.Server.InspectionClient import runClient
    #Create a socket and bind it to 0 to find a free port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, 0))
    PORT = s.getsockname()[1]
    s.close()
    #Pass the port no as an argument to the popen
    serverpath = os.path.join(os.path.dirname(inspect.getsourcefile(runClient)), 'DiracProcess.py')
    popen_cmd = ['python',serverpath, str(PORT)]
    dirac_process = subprocess.Popen(popen_cmd, env = getDiracEnv(), stdin=subprocess.PIPE)
    global running_dirac_process
    running_dirac_process = (dirac_process.pid, PORT)

    #Now set a random string to make sure only commands from this sessions are executed
    rand_hash = uuid.uuid4()
    global dirac_process_ids
    dirac_process_ids = (dirac_process.pid, PORT, rand_hash)
    #Pipe the random string without waiting for the process to finish.
    dirac_process.stdin.write(str(rand_hash).encode("utf-8"))
    dirac_process.stdin.close()

    data = ''
    #We have to wait a little bit for the subprocess to start the server so we try until the connection stops being refused. Set a limit of one minute.
    connection_timeout = time.time() + 60
    started = False
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while time.time()<connection_timeout and not started:
        try:
            s.connect((HOST, PORT))
            started = True
        except socket.error as serr:
            time.sleep(1)
    if not started:
        raise GangaDiracError("Failed to start the Dirac server process!")
    #Now setup the Dirac environment in the subprocess
    dirac_command = str(rand_hash)
    dirac_command = dirac_command + getDiracCommandIncludes()
    dirac_command = dirac_command + end_trans
    s.sendall(dirac_command.encode("utf-8"))
    data = s.recv(1024)
    s.close()

exportToGPI('startDiracProcess', startDiracProcess, 'Functions')

def stopDiracProcess():
    '''
    Stop the Dirac process if it is running
    '''
    global running_dirac_process
    if running_dirac_process:
        logger.info('Stopping the DIRAC process')
        dirac_process.kill()
        running_dirac_process = False

exportToGPI('stopDiracProcess', stopDiracProcess, 'Functions')

def diracAPI_interactive(connection_attempts=5):
    '''
    Run an interactive server within the DIRAC environment.
    '''

    from GangaDirac.Lib.Server.InspectionClient import runClient
    serverpath = os.path.join(os.path.dirname(inspect.getsourcefile(runClient)), 'InspectionServer.py')
    from GangaCore.Core.GangaThread.WorkerThreads import getQueues
    getQueues().add(execute("execfile('%s')" % serverpath, timeout=None, shell=False))

    #time.sleep(1)
    sys.stdout.write( "\nType 'q' or 'Q' or 'exit' or 'exit()' to quit but NOT ctrl-D")
    i = 0
    excpt = None
    while i < connection_attempts:
        try:
            runClient()
            break
        except:
            if i == (connection_attempts - 1):
                excpt = traceback.format_exc()
        finally:
            i += 1
    return excpt
exportToGPI('diracAPI_interactive', diracAPI_interactive, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#


def diracAPI_async(cmd, timeout=120):
    '''
    Execute DIRAC API commands from w/in GangaCore.
    '''
    from GangaCore.Core.GangaThread.WorkerThreads import getQueues
    return getQueues().add(execute(cmd, timeout=timeout))

exportToGPI('diracAPI_async', diracAPI_async, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#


def getDiracFiles():
    from GangaDirac.Lib.Files.DiracFile import DiracFile
    from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
    filename = DiracFile.diracLFNBase().replace('/', '-') + '.lfns'
    logger.info('Creating list, this can take a while if you have a large number of SE files, please wait...')
    execute('dirac-dms-user-lfns &> /dev/null', shell=True, timeout=None)
    g = GangaList()
    with open(filename[1:], 'r') as lfnlist:
        lfnlist.seek(0)
        g.extend((DiracFile(lfn='%s' % lfn.strip()) for lfn in lfnlist.readlines()))
    return addProxy(g)

exportToGPI('getDiracFiles', getDiracFiles, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#


def dumpObject(object, filename):
    '''
    These are complimentary functions to export/load which are already exported to
    the GPI from GangaCore.GPIDev.Persistency. The difference being that these functions will
    export the objects using the pickle persistency format rather than a Ganga streaming
    (human readable) format.
    '''
    try:
        with open(os.path.expandvars(os.path.expanduser(filename)), 'wb') as f:
            pickle.dump(stripProxy(object), f)
    except:
        logger.error("Problem when dumping file '%s': %s" % (filename, traceback.format_exc()))
exportToGPI('dumpObject', dumpObject, 'Functions')


def loadObject(filename):
    '''
    These are complimentary functions to export/load which are already exported to
    the GPI from GangaCore.GPIDev.Persistency. The difference being that these functions will
    export the objects using the pickle persistency format rather than a Ganga streaming
    (human readable) format.
    '''
    try:
        with open(os.path.expandvars(os.path.expanduser(filename)), 'rb') as f:
            r = pickle.load(f)
    except:
        logger.error("Problem when loading file '%s': %s" % (filename, traceback.format_exc()))
    else:
        return addProxy(r)
exportToGPI('loadObject', loadObject, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#
