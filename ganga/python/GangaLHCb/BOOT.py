
def browseBK():
    import Ganga.Utility.logging
    logger = Ganga.Utility.logging.getLogger()
    try: 
        import GangaGUI.LHCB_BKDB_browser.browser_mod
    except ImportError:
        logger.warning("Could not import the GangaGUI. ")
        logger.warning("The LHCb bookkeeping browser will not be available")
        logger.warning("Try to add 'GangaGUI' to your RUNTIME_PATH")
        logger.warning("in your .gangarc file or start ganga with options:")
        logger.warning('''"ganga -o'[Configuration]RUNTIME_PATH=GangaGUI'"''')
        return None
    list = GangaGUI.LHCB_BKDB_browser.browser_mod.browse()
    return LHCbDataset(files=[File(name=n) for n in list])

        
    
