###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ElapsedTimeProfiler.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
###############################################################################
#
# LCG backend profiler
#
# ATLAS/ARDA
#
# Date:   November 2007
import time
from GangaCore.Utility.logging import getLogger


class ElapsedTimeProfiler(object):

    '''Elapsed time profiler'''

    def __init__(self, logger=None):

        if not logger:
            logger = getLogger(name='GangaCore.Lib.LCG.ElapsedTimeProfiler')

        self.logger = logger

    def start(self):
        self.beg = time.time()

    def check(self, message):
        etime = time.time() - self.beg
        self.logger.debug('%s: %f sec.' % (message, etime))

    def checkAndStart(self, message):
        self.check(message)
        self.start()
