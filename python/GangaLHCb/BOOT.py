

def browseBK(gui=True):
    """
Utility function to launch the new LHCb bookkeeping from inside Ganga.
The function returns an LHCbDataset object. 

After browsing and selecting the desired datafiles, click on the
"Save as ..." button. The Browser will quit and save the seleted files
as an LHCbDataset object

Usage:
# retrieve an LHCbDataset object with the selected files and store
# them in the variable l
l=browseBK()

# retrieve an LHCbDataset object with the selected files and store
# them in the jobs inputdata field, ready for submission
j.inputdata=browseBK()    
    """
    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()
    try: 
        from GangaLHCb.Lib.Dirac.Bookkeeping import Bookkeeping
        from GangaLHCb.Lib.LHCbDataset import LHCbDataset
    except ImportError:
        logger.warning('''Could not start Bookkeeping Browser''')
        return None
    bkk=Bookkeeping()
    return  bkk.browse(gui)

        
    
