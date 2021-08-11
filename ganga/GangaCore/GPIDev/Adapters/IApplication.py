##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IApplication.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
##########################################################################

from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import Schema, Version

import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()


class PostprocessStatusUpdate(Exception):

    """ This exception may be raised by postprocess hooks to change the job status."""

    __slots__ = ('status',)

    def __init__(self, status):
        Exception.__init__(self)
        self.status = status


class IApplication(GangaObject):

    """
     Base class for all application objects. Derived classes represent
     logical  applications  in  the  GPI and  implement  configuration
     handler functionality. The application configuration is the first
     phase of job submission.

     General rules for implementing the configure methods:

     In  general the  configure() and  master_configure()  methods are
     called always in  the context of job submission,  so in principle
     you may navigate to  the associated job object (including backend
     information). However it  is not advised to use  backend or extra
     sandbox  information at this  point.  Code  which depends  on the
     backend  should be  put in  application-specific  runtime handler
     which is the next step of job submission.

     The input/output dataset information may be used if neccessary.
     Objects in the job object tree should not be modified.

     Efficient implementation of splitting:

     If you want to enable the  typical case of splitting based on the
     dataset  (defined at  the  job  level) then  it  is very  simple:
     configure()  should only process  the inputdata  part of  the job
     configuration and master_configure() should  do the rest. In that
     case the splitter should not mutate the application object in the
     subjobs, because such changes will not be taken into account (and
     framework will have inconsistent behaviour).

     You  may  also take  an  extreme  approach  and move  the  entire
     application    configuration     to    configure().     Arbitrary
     modifications of the application  object in the subjobs which are
     done by  the splitter will  take effect.  But if  the application
     configuration  process is time  consuming it  will be  repeated a
     number of times which is inneficient.

     Otherwise  you   should  first   identify  which  parts   of  the
     application  object may be  altered by  the splitter  and process
     them  in  configure()   method.   The  master_configure()  should
     perform only  the time-consuming part of  the configuration which
     is shared among the subjobs.  This means that splitter should not
     try to  modify the application  parameters which are used  in the
     common master_configure().  """

    _schema = Schema(Version(0, 0), {})
    _category = 'applications'
    _hidden = 1

    __slots__ = list()

    def __init__(self):
        super(IApplication, self).__init__()

    def master_configure(self):
        """ Configure the shared/master  aspect of the application.
        Return a tuple (modified_flag,  appconfig).

        This method is always called exactly once, also in the case of
        splitting.

        Return tuple:

          - appconfig  (also  known   as  appextra)  is  an  arbitrary
            structure which will  be processed by application-specific
            runtime handler as the next step of job submission.

          - modified_flag  is True  if application  object  (self) has
            been modified during configure()

        If this method is not implemented in the derived class then it
        is ignored.  """

        return (0, None)

    def configure(self, master_appconfig):
        """ Configure  the specific aspect of the  application .  This
        method has a similar  meaning as master_configure() method and
        it should return a tuple (modified_flag,appconfig).

        This method must be  implemented in a derived class. Otherwise
        the submission will fail.

        Arguments:
          - master_appconfig is a result of the master job master_configure()
            method.

        In  case of  splitting this  method  will be  called for  each
        subjob  object  exactly  once  which  means that  it  will  be
        executed as many times as there are subjobs.

        If there  is no splitting  this method will be  called exactly
        once, after  master_configure().

        Transition from Ganga 4.0.x:
         - master_appconfig should be ignored (it is None anyway)
        """

        raise NotImplementedError

    def postprocess(self):
        """ Postprocessing after the job was reported as completed. By default do nothing.
        This method may raise an exception PostprocessStatusUpdate('failed'). In this
        case the job status will be 'failed'. The postprocess_failed() hook will NOT be called."""
        pass

    def postprocess_failed(self):
        """ Postprocessing after the job was reported as failed. By default do nothing."""
        pass

    def transition_update(self, new_status):
        """
        This method will be called just before the status of the parent Job changes to new_status.
        The default it to do nothing.
        """
        pass

