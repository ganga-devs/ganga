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
    diracAPI(\'print Dirac().status([66])\')
    diracAPI(\'print dirac.status([66])\')

    # or can achieve the same using command defined and included from
    # getConfig('DIRAC')['DiracCommandFiles']
    diracAPI(\'status([66])\')

    '''
    return execute(cmd, timeout=timeout, cred_req=cred_req)

exportToGPI('diracAPI', diracAPI, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#


def diracAPI_interactive(connection_attempts=5):
    '''
    Run an interactive server within the DIRAC environment.
    '''
    import os
    import sys
    import time
    import inspect
    import traceback
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
    import os
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
    import os
    import pickle
    import traceback
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
    import os
    import pickle
    import traceback
    try:
        with open(os.path.expandvars(os.path.expanduser(filename)), 'rb') as f:
            r = pickle.load(f)
    except:
        logger.error("Problem when loading file '%s': %s" % (filename, traceback.format_exc()))
    else:
        return addProxy(r)
exportToGPI('loadObject', loadObject, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#
