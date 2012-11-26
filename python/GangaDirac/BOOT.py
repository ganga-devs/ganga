
from Ganga.Runtime.GPIexport import exportToGPI

    
def diracAPI(cmd, priority=5, timeout = 60):
    '''Execute DIRAC API commands from w/in Ganga.

    The value of local variable \"result\" will be returned, e.g.:
    
    # this will simply return 87
    diracAPI(\'result = 87\')
    
    # this will return the status of job 66
    diracAPI(\'result = Dirac().status(66)\')

    If \"result\" is not set, then the commands are still executed but no value
    is returned.
    '''
    #    from GangaDirac.Lib.Backends.Dirac import Dirac
    from Ganga.GPI import Dirac
    return Dirac._impl.execAPI(cmd, priority, timeout)
    #from GangaDirac.Lib.Backends.DiracBase import DiracBase
    #return DiracBase.execAPI(cmd, priority, timeout)

exportToGPI('diracAPI',diracAPI,'Functions')

def diracQueue(priority=5, timeout = 60):
    '''Return the current local DIRAC server queue.'''
    return diracAPI('###GET-QUEUE###',priority,timeout)

exportToGPI('diracQueue',diracQueue,'Functions')

def killDiracServer():
    '''Kills the child proces which runs the DIRAC server.'''
    #from GangaDirac.Lib.Backends.Dirac import Dirac
    #from GangaDirac.Lib.Backends.DiracBase import DiracBase
    from Ganga.GPI import Dirac
    return Dirac._impl.killServer()

exportToGPI('killDiracServer',killDiracServer,'Functions')    
