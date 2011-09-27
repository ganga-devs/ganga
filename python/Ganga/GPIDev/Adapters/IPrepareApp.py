################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IApplication.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
################################################################################

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory



from Ganga.GPIDev.Schema import *

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


class IPrepareApp(IApplication):

    """
     common master_configure().  """

    _schema =  Schema(Version(0,0), {})
    _category='applications'
    _name = 'PrepareApp'
    _hidden = 1

    def _readonly(self):
        """An application is read-only once it has been prepared."""
        if self.is_prepared is None:
            return 0
        else:
            logger.error("Cannot modify a prepared application's attributes. First unprepare() the application.")
            return 1


    def prepare(self, force=False):
        """
        Base class for all applications which can be placed into a prepared\
        state. 

        """
        pass


    def unprepare(self, force=False):
        """
        Revert an application back to the exact state it was in prior to being\
        prepared.
        """
        pass


    def incrementShareCounter(self, shared_directory_name):
        logger.info('Incrementing shared directory reference counter')
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.increase(shared_directory_name)


    def decrementShareCounter(self, shared_directory_name):
        logger.info('Decrementing shared directory reference counter')
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        shareref.decrease(shared_directory_name)
