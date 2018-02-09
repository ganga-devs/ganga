##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IRuntimeHandler.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################

import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()


class IRuntimeHandler(object):

    """ The RuntimeHandler is a connector between the application and the backend.

    Application configure methods produce appconfig objects. Backend submit method
    consumes the jobconfig object. RuntimeHandler translates the appconfig objects
    into the jobconfig objects. The translation is a part of the job submission.
    It is implemented by the prepare methods.

    """

    __slots__ = list()

    def master_prepare(self, app, appmasterconfig):
        """ Prepare  the shared/master aspect of  the job submission.
        Called  once  per  job  (both  split and  not-split).  If  the
        preparation contains some expensive actions it may be factored
        out in this method.

        Return value:
         - jobmasterconfig object understood by backend

        Arguments:
          - app : original application object
          - appmaster config : result of app.master_configure()

        """
        return None

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        """ Prepare the specific/subjob  aspect of the job submission.
        Called  once per  subjob if  splitting enabled.   If splitting
        disabled called  exactly once (the master  and specific aspect
        configured on the same job).

        Return value:
         - subjobconfig list of objects understood by backends

        Arguments:
          - app : original application object
          - appmaster config : result of app.master_configure()

         - appsubconfig : a list of results of app.configure() for each subjob
                         (or a master job if no splitting)
         - jobmasterconfig : a result of self.master_prepare()
        """

        raise NotImplementedError
