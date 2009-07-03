
from Ganga.Runtime.GPIexport import exportToGPI

def browseBK(gui=True):
    """Return an LHCbDataset from the GUI LHCb Bookkeeping.

    Utility function to launch the new LHCb bookkeeping from inside Ganga.
    The function returns an LHCbDataset object. 

    After browsing and selecting the desired datafiles, click on the
    \"Save as ...\" button. The Browser will quit and save the seleted files
    as an LHCbDataset object

    Usage:
    # retrieve an LHCbDataset object with the selected files and store
    # them in the variable l
    l = browseBK()

    # retrieve an LHCbDataset object with the selected files and store
    # them in the jobs inputdata field, ready for submission
    j.inputdata=browseBK()    
    """
    import Ganga.Utility.logging
    from Ganga.GPIDev.Base.Proxy import addProxy
    logger = Ganga.Utility.logging.getLogger()
    try: 
        from GangaLHCb.Lib.DIRAC.Bookkeeping import Bookkeeping
        from Ganga.GPI import LHCbDataset
    except ImportError:
        logger.warning('Could not start Bookkeeping Browser')
        return None
    bkk = Bookkeeping()
    return  addProxy(bkk.browse(gui))

exportToGPI('browseBK',browseBK,'Functions')        
    
def diracAPI(cmd,timeout=None):
    '''Execute DIRAC API commands from w/in Ganga.

    The value of local variable \"result\" will be returned, e.g.:
    
    # this will simply return 87
    diracAPI(\'result = 87\')
    
    # this will return the status of job 66
    diracAPI(\'result = Dirac().status(66)\')

    If \"result\" is not set, then the commands are still executed but no value
    is returned.
    '''
    from GangaLHCb.Lib.DIRAC.Dirac import Dirac
    return Dirac.execAPI(cmd,timeout)

exportToGPI('diracAPI',diracAPI,'Functions')

def killDiracServer():
    '''Kills the child proces which runs the DIRAC server.'''
    from GangaLHCb.Lib.DIRAC.Dirac import Dirac
    return Dirac.killServer()

exportToGPI('killDiracServer',killDiracServer,'Functions')    
