from GangaCore.Runtime.GPIexport import exportToGPI

from GangaCore.Utility.logging import getLogger
logger = getLogger(modulename=True)


def browseBK(gui=True):
    """Return an LHCbDataset from the GUI LHCb Bookkeeping.

    Utility function to launch the new LHCb bookkeeping from inside GangaCore.
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
    import GangaCore.Utility.logging
    from GangaCore.GPIDev.Base.Proxy import addProxy
    logger = GangaCore.Utility.logging.getLogger()
    try:
        from GangaLHCb.Lib.Backends.Bookkeeping import Bookkeeping
        from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
    except ImportError:
        logger.warning('Could not start Bookkeeping Browser')
        return None
    bkk = Bookkeeping()
    return addProxy(bkk.browse(gui))

exportToGPI('browseBK', browseBK, 'Functions')


def fixBKQueryInBox(newCategory='query'):
    import os
    from GangaCore.Utility.Config import getConfig

    def _filt(line):
        return 'class name=\"BKQuery\"' in line

    gangadir = getConfig('Configuration')['gangadir']
    logger.info('found gangadir = ' + gangadir)
    for root, dirs, files in os.walk(gangadir):
        if 'data' in files and 'box' in root and not 'box.' in root:
            path = os.path.join(root, 'data')
            logger.info("looking at " + path)
            f1 = open(path, 'r')
            f2 = open(path + '~', 'r')
            lines1 = f1.readlines()
            lines2 = f2.readlines()
            line1 = list(filter(_filt, lines1))
            line2 = list(filter(_filt, lines2))
            f1.close()
            f2.close()

            newline = ' <class name=\"BKQuery\" version=\"1.2\" category=\"%s\">\n' % newCategory
            if len(line1) is 1:
                lines1[lines1.index(line1[0])] = newline
                logger.info('backing up old settings...')
                os.system('cp %s %sX; mv %s~ %s~X' % (path, path, path, path))
                f1 = open(path, 'w')
                f2 = open(path + '~', 'w')
                for l in lines1:
                    f1.write(l)
                    f2.write(l)
                f1.close()
                f2.close()
                p = os.path.join(root[:-2], root.split('/')[-1] + '.index')
                os.system('rm -f %s' % p)
            elif len(line2) is 1:
                lines2[lines2.index(line2[0])] = newline
                logger.info('backing up old settings...')
                os.system('cp %s %sX; mv %s~ %s~X' % (path, path, path, path))
                f1 = open(path, 'w')
                f2 = open(path + '~', 'w')
                for l in lines2:
                    f1.write(l)
                    f2.write(l)
                f1.close()
                f2.close()
                p = os.path.join(root[:-2], root.split('/')[-1] + '.index')
                os.system('rm -f %s' % p)
    logger.info("box repository converted!\n")
    logger.info(
        "PLEASE NOW QUIT THIS GANGA SESSION AND RESTART TO SEE EFFECTS.")

exportToGPI('fixBKQueryInBox', fixBKQueryInBox, 'Functions')


def restoreOLDBox():
    import os
    from GangaCore.Utility.Config import getConfig
    gangadir = getConfig('Configuration')['gangadir']
    logger.info('found gangadir = ' + gangadir)
    for root, dirs, files in os.walk(gangadir):
        if 'dataX' in files and 'box' in root and not 'box.' in root:
            path = os.path.join(root, 'data')
            logger.info("restoring old BKQuery box file...")
            os.system('mv %sX %s; mv %s~X %s~' % (path, path, path, path))

    logger.info("box repository converted!\n")
    logger.info(
        "PLEASE NOW QUIT THIS GANGA SESSION AND RESTART TO SEE EFFECTS.")

exportToGPI('restoreOLDBox', restoreOLDBox, 'Functions')
