def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}
    
def loadPlugins(c):

    import os,sys
    from GangaCore.Utility.logging import getLogger
    logger = getLogger()
    logger.info('You are now using Python %s',sys.version.split()[0])

    import GangaPlotter.Plotter
    from Plotter.GangaPlotter import GangaPlotter
    #GangaPlotter.Plotter.plotter = GangaPlotter()
    plotter = GangaPlotter()

    from GangaCore.Runtime.GPIexport import exportToGPI

    #exportToGPI('plotter',GangaPlotter.Plotter.plotter,'Objects','graphics plotter')
    exportToGPI('plotter',plotter,'Objects','graphics plotter')
