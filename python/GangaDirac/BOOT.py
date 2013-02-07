
from Ganga.Runtime.GPIexport import exportToGPI
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
logger = getLogger()
    
def diracAPI(cmd, timeout = 60):
    '''Execute DIRAC API commands from w/in Ganga.

    The value of local variable \"result\" will be returned, e.g.:
    
    # this will simply return 87
    diracAPI(\'print 87\')
    
    # this will return the status of job 66
    diracAPI(\'status([66])\')

    '''
    #    from GangaDirac.Lib.Backends.Dirac import Dirac
    from Ganga.GPI import Dirac
    return Dirac._impl.execAPI(cmd, timeout)
    #from GangaDirac.Lib.Backends.DiracBase import DiracBase
    #return DiracBase.execAPI(cmd, priority, timeout)

exportToGPI('diracAPI',diracAPI,'Functions')

def diracQueue():
    '''Return the current local DIRAC server queue.'''
    from Ganga.GPI import Dirac
    return Dirac._impl.getQueue()


exportToGPI('diracQueue',diracQueue,'Functions')

def killDiracServer():
    '''Kills the child proces which runs the DIRAC server.'''
    #from GangaDirac.Lib.Backends.Dirac import Dirac
    #from GangaDirac.Lib.Backends.DiracBase import DiracBase
    from Ganga.GPI import Dirac
    return Dirac._impl.killServer()

exportToGPI('killDiracServer',killDiracServer,'Functions')    


def getDiracFiles():
    import os
    from Ganga.GPI import DiracFile
    from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
    filename = getConfig('DIRAC')['DiracLFNBase'].replace('/','-') + '.lfns'
    logger.info('Creating list, please wait...')
    os.system('dirac-dms-user-lfns &> /dev/null')
    g=GangaList()
    with open(filename[1:],'r') as lfnlist:
        lfnlist.seek(0)
        g.extend((DiracFile(lfn='%s'% lfn.strip()) for lfn in lfnlist.readlines()))
    return addProxy(g)
 
exportToGPI('getDiracFiles',getDiracFiles,'Functions')


def dumpObject(object, filename):
    import pickle
    f=open(filename, 'wb')
    pickle.dump(stripProxy(object), f)
    f.close()
exportToGPI('dumpObject',dumpObject,'Functions')

def loadObject(filename):
    import pickle
    f=open(filename, 'rb')
    r = pickle.load(f)
    f.close()
    return addProxy(r)
exportToGPI('loadObject',loadObject,'Functions')
