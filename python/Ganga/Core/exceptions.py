##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: exceptions.py,v 1.2 2008-09-09 14:37:16 moscicki Exp $
##########################################################################


class GangaException(Exception):

    """ Markup base class for well-behaved exception that should not print the whole traceback to user's prompt
        Any subclass of this exception is handled by a custom IPython exception handler
        and is printed out in an usable format to iPython prompt
    """
    logger = None

    def __init__(self, *args, **kwds):
        super(GangaException, self).__init__(args)
        Exception.__init__(self, *args)
        self.kwds = kwds

        # This code will give a stack trace from a GangaException only when debugging is enabled
        # This makes debugging what's going on much easier whilst hiding mess
        # from users
        #if self.logger is None:
        #    from Ganga.Utility.logging import getLogger
        #    self.logger = getLogger()

        #import logging
        #if self.logger.isEnabledFor(logging.DEBUG):
        #    import traceback
        #    traceback.print_stack()

    def __str__(self):
        """
         String representation of this class
        """
        from Ganga.GPIDev.Base.Proxy import getName
        _str = "%s: " % getName(self)
        if hasattr(self, 'args') and self.args:
            _str += " %s" % str(self.args)
        if hasattr(self, 'kwds') and self.kwds:
            _str += " %s" % str(self.kwds)
        return _str

class PluginError(GangaException):
    """
    Class to be used in 1 place only in loading plugins
    """


class ApplicationConfigurationError(GangaException):

    def __init__(self, excpt, message):
        GangaException.__init__(self, excpt, message)
        self.message = message
        self.excpt = excpt

    def __str__(self):
        if self.excpt:
            err = '(%s:%s)' % (str(type(self.excpt)), str(self.excpt))
        else:
            err = ''
        return "ApplicationConfigurationError: %s %s" % (self.message, err)


class ApplicationPrepareError(GangaException):
    pass


class BackendError(GangaException):

    def __init__(self, backend_name, message):
        GangaException.__init__(self, backend_name, message)
        self.backend_name = backend_name
        self.message = message

    def __str__(self):
        return "BackendError: %s (%s backend) " % (self.message, self.backend_name)


class IncompleteJobSubmissionError(GangaException):

    def __init__(self, *args):
        GangaException.__init__(self, *args)


class IncompleteKillError(GangaException):

    def __init__(self, *args):
        GangaException.__init__(self, *args)


class JobManagerError(GangaException):

    def __init__(self, msg):
        self.msg = msg
        GangaException.__init__(self, msg)

    def __str__(self):
        return "JobManagerError: %s" % str(self.msg)


class GangaAttributeError(AttributeError, GangaException):
    logger = None

    def __init__(self, *a, **k):
        GangaException.__init__(self, *a, **k)
        AttributeError.__init__(self, *a, **k)

        # This code will give a stack trace from a GangaException only when debugging is enabled
        # This makes debugging what's going on much easier whilst hiding mess
        # from users
        #if self.logger is None:
        #    from Ganga.Utility.logging import getLogger
        #    self.logger = Ganga.Utility.logging.getLogger()

        #    import logging
        #    if self.logger.isEnabledFor(logging.DEBUG):
        #        import traceback
        #        traceback.print_stack()


class GangaValueError(ValueError, GangaException):

    def __init__(self, *a, **k):
        GangaException.__init__(self, *a, **k)
        ValueError.__init__(self, *a, **k)


class GangaIOError(IOError, GangaException):
    pass


class SplitterError(GangaException):
    """Splitting errors."""

    def __init__(self, message=''):
        GangaException.__init__(self, message)
        self.message = message

    def __str__(self):
        return "SplitterError: %s " % self.message


class ProtectedAttributeError(GangaAttributeError):

    'Attribute is read-only and may not be modified by the user (for example job.id)'

    def __init__(self, *a, **k):
        GangaAttributeError.__init__(self, *a, **k)


class ReadOnlyObjectError(GangaAttributeError):

    'Object cannot be modified (for example job in a submitted state)'

    def __init__(self, *a, **k):
        GangaAttributeError.__init__(self, *a, **k)


class TypeMismatchError(GangaAttributeError):

    def __init__(self, *a, **k):
        GangaAttributeError.__init__(self, *a, **k)


class SchemaError(GangaAttributeError):

    def __init__(self, *a, **k):
        GangaAttributeError.__init__(self, *a, **k)


class SchemaVersionError(GangaException):
    """
    Error raised on schema version error
    """
    def __init__(self, what=''):
        """
        This is the error thrown when the Schema version in XML doesn't match what's in the code. This may be dropped in a future release
        Args:
            what (str): This is a string of what went wrong
        """
        super(SchemaVersionError, self).__init__(what)
        self.what = what

    def __str__(self):
        return "SchemaVersionError: %s" % self.what


class InaccessibleObjectError(GangaException):

    def __init__(self, repo=None, id='', orig=None):
        """
        This is an error in accessing an object in the repo
        Args:
            repo (GangaRepository): The repository the error happened in
            id (int): The key of the object in the objects dict where this happened
            orig (exception): The original exception
        """
        super(InaccessibleObjectError, self).__init__("Inaccessible Object: %s" % id)
        self.repo = repo
        self.id = id
        self.orig = orig

    def __str__(self):
        if str(self.orig).find('comments') > -1:
            return "Please restart Ganga in order to reload the object"
        else:
            from Ganga.GPIDev.Base.Proxy import getName
            return "Repository '%s' object #%s is not accessible because of an %s: %s" % (self.repo.registry.name, self.id, getName(self.orig), str(self.orig))


class RepositoryError(GangaException):

    """This error is raised if there is a fatal error in the repository."""

    def __init__(self, repo=None, what=''):
        """
        This is a fatal repo error
        Args:
            repo (GangaRepository): The repository the error happened in
            what (str): The original exception/error/description
        """
        super(RepositoryError, self).__init__(self, what)
        self.what = what
        self.repository = repo
        from Ganga.Utility.logging import getLogger
        logger = getLogger()
        logger.error("A severe error occurred in the Repository '%s': %s" % (repo.registry.name, what))
        logger.error('If you believe the problem has been solved, type "reactivate()" to re-enable ')
        try:
            from Ganga.Core.InternalServices.Coordinator import disableInternalServices
            disableInternalServices()
            from Ganga.Core.GangaThread.WorkerThreads import shutdownQueues
            shutdownQueues()
            logger.error("Shutting Down Repository_runtime")
            from Ganga.Runtime import Repository_runtime
            repository_runtime.shutdown()
        except:
            logger.error("Unable to disable Internal services, they may have already been disabled!")


# Exception raised by the Ganga Repository
class BulkOperationRepositoryError(RepositoryError):

    """
    For bulk operations this exception
    have a non-empty dictionary 'details'
    which contains ids of failed jobs as keys and 'original' exceptions as values.
    """

    def __init__(self, details=None, msg=None):
        if msg == None:
            msg = "RepositoryError: %s" % str(err)
            RepositoryError.__init__(self, None, msg)
            self.details = details
            if details == None:
                self.details = {}

    def listFailedJobs(self):
        return self.details.keys()

    def getOriginalJobError(self, id):
        return self.details.get(id)

#
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2008/07/17 16:40:49  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.20.4.1  2008/03/04 14:49:03  amuraru
# make RepositoryError a GangaException
#
# Revision 1.20  2007/09/13 08:36:20  amuraru
# fixed the _str_ method of GangaException
#
# Revision 1.19  2007/09/12 16:25:09  amuraru
# - log user exceptions using specific loggers
# - fixed the str repr in exceptions
#
# Revision 1.18  2007/03/26 16:10:47  moscicki
# formating of exception messages
#
# Revision 1.17  2007/02/28 18:23:59  moscicki
# moved GangaException here (it now inherits from Exception)
#
# Revision 1.16  2007/02/22 13:25:29  moscicki
# define JobManager exception here
#
# Revision 1.15  2006/10/26 12:31:34  moscicki
# minor repository exception changes
#
# Revision 1.14  2006/08/11 13:41:54  moscicki
# better formatting of messages
#
# Revision 1.13  2006/07/10 14:02:00  moscicki
# IncompleteKillError
#
# Revision 1.12  2006/02/10 14:07:42  moscicki
# __str__ for ApplicationConfigurationError
#
# Revision 1.11  2005/12/02 15:23:11  moscicki
# IcompleteJobSubmission
#
# Revision 1.10  2005/08/23 17:20:59  moscicki
# *** empty log message ***
#
# Revision 1.9  2005/08/23 17:20:33  moscicki
# *** empty log message ***
#
#
#
