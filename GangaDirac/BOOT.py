from Ganga.Runtime.GPIexport                         import exportToGPI
from Ganga.GPIDev.Base.Proxy                         import addProxy, stripProxy
from Ganga.Utility.Config                            import getConfig
from Ganga.Utility.logging                           import getLogger
from GangaDirac.Lib.Server.WorkerThreadPool          import WorkerThreadPool
from GangaDirac.Lib.Utilities.ThreadPoolQueueMonitor import ThreadPoolQueueMonitor
from GangaDirac.Lib.Utilities.DiracUtilities         import execute
logger = getLogger()
user_threadpool       = WorkerThreadPool()
monitoring_threadpool = WorkerThreadPool()
queues_threadpoolMonitor = ThreadPoolQueueMonitor(user_threadpool, monitoring_threadpool)
exportToGPI('queues', queues_threadpoolMonitor, 'Objects')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#
def diracAPI(cmd, timeout = 60):
    '''Execute DIRAC API commands from w/in Ganga.

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
    return execute(cmd, timeout=timeout)

exportToGPI('diracAPI', diracAPI, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#
def diracAPI_interactive(connection_attempts = 5):
    '''
    Run an interactive server within the DIRAC environment.
    '''
    import os, time,inspect,traceback
    from GangaDirac.Lib.Server.InspectionClient import runClient
    serverpath = os.path.join(os.path.dirname(inspect.getsourcefile(runClient)),'InspectionServer.py')
    user_threadpool.add_process("execfile('%s')"%serverpath, timeout=None, shell=False, priority = 1)
    
    time.sleep(1)
    print "\nType 'q' or 'Q' or 'exit' or 'exit()' to quit but NOT ctrl-D"
    i=0
    excpt = None
    while i < connection_attempts:
        try:
            runClient()
            break
        except:
            if i==(connection_attempts - 1):
                excpt=traceback.format_exc()
        finally:
            ++i
    return excpt
exportToGPI('diracAPI_interactive', diracAPI_interactive, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#
def diracAPI_async(cmd, timeout = 120):
    '''
    Execute DIRAC API commands from w/in Ganga.
    '''
    return user_threadpool.add_process(cmd, timeout=timeout, priority = 2)

exportToGPI('diracAPI_async', diracAPI_async, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#
def getDiracFiles():
    import os
    from Ganga.GPI import DiracFile
    from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
    filename = getConfig('DIRAC')['DiracLFNBase'].replace('/','-') + '.lfns'
    logger.info('Creating list, this can take a while if you have a large number of SE files, please wait...')
    execute('dirac-dms-user-lfns &> /dev/null', shell=True, timeout=None)
    g=GangaList()
    with open(filename[1:],'r') as lfnlist:
        lfnlist.seek(0)
        g.extend((DiracFile(lfn='%s'% lfn.strip()) for lfn in lfnlist.readlines()))
    return addProxy(g)
 
exportToGPI('getDiracFiles', getDiracFiles, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#
def dumpObject(object, filename):
    '''
    These are complimentary functions to export/load which are already exported to
    the GPI from Ganga.GPIDev.Persistency. The difference being that these functions will
    export the objects using the pickle persistency format rather than a Ganga streaming
    (human readable) format.
    '''
    import os, pickle, traceback
    try:
        with open(os.path.expandvars(os.path.expanduser(filename)), 'wb') as f:
            pickle.dump(stripProxy(object), f)
    except:
        logger.error("Problem when dumping file '%s': %s" % (filename, traceback.format_exc()) )
exportToGPI('dumpObject', dumpObject, 'Functions')

def loadObject(filename):
    '''
    These are complimentary functions to export/load which are already exported to
    the GPI from Ganga.GPIDev.Persistency. The difference being that these functions will
    export the objects using the pickle persistency format rather than a Ganga streaming
    (human readable) format.
    '''
    import os, pickle, traceback
    try:
        with open(os.path.expandvars(os.path.expanduser(filename)), 'rb') as f:
            r = pickle.load(f)
    except:
        logger.error("Problem when loading file '%s': %s" % (filename, traceback.format_exc()) )
    else:
        return addProxy(r)
exportToGPI('loadObject', loadObject, 'Functions')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/#
