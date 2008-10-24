
def browseBK(gui=True):
    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()
    try: 
        from GangaLHCb.Lib.Dirac.Bookkeeping import Bookkeeping
    except ImportError:
        logger.warning('''Could not start Bookkeeping Browser''')
        return None
    bkk=Bookkeeping()
    list = bkk.browse(gui)
    return LHCbDataset(files=[File(name=n) for n in list])

        
    
