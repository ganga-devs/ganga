"""A set of Core Ganga specific Exceptions

This module contains all the Ganga Core specific exceptions used throughout the codebase.

"""

class GangaException(Exception):
    """Basic Ganga Exception class"""
    __slots__=list()

    def __str__(self):
        """
        Return a string containing the error type as well as the parent string
        """
        return "%s: %s" % (self.__class__.__name__, super(GangaException, self).__str__())


class GangaFileError(GangaException):
    """
    This is intended to be thrown as an IGangaFile Error during Runtime
    """

class GangaDiskSpaceError(GangaException):
    """
    Specific for issues with running out of disk space
    """

class PluginError(GangaException):
    """
    Class to be used in 1 place only in loading plugins
    """


class ApplicationConfigurationError(GangaException):
    """Specific Application Configuration Exception"""


class ApplicationPrepareError(GangaException):
    """Exception during Application preparation"""


class BackendError(GangaException):
    """Exception from backend code that gives the offending backend in the message"""

    def __init__(self, backend_name, message):
        GangaException.__init__(self, message)
        self.backend_name = backend_name

    def __str__(self):
        # print the backend name as well as error
        return "BackendError: %s (%s backend) " % (self.args[0], self.backend_name)


class IncompleteJobSubmissionError(GangaException):
    """Exception during submission"""


class IncompleteKillError(GangaException):
    """Exception during Kill operation"""


class JobManagerError(GangaException):
    """Exception for failed submission/configuration"""


class GangaAttributeError(GangaException, AttributeError):
    """Exception raised for bad attributes"""


class GangaValueError(GangaException):
    """Error for assigning an incorrect value to a schema item"""


class GangaIOError(GangaException):
    """Exception for IO errors"""


class SplitterError(GangaException):
    """Exception raised during job splitting"""


class ProtectedAttributeError(GangaAttributeError):
    """Attribute is read-only and may not be modified by the user (for example job.id)"""


class ReadOnlyObjectError(GangaAttributeError):
    """Object cannot be modified (for example job in a submitted state)"""


class TypeMismatchError(GangaAttributeError):
    """Exception due to a mismatch of types"""


class SchemaError(GangaAttributeError):
    """Error in the schema items"""


class SchemaVersionError(GangaException):
    """Error raised on schema version error"""


class InaccessibleObjectError(GangaException):

    def __init__(self, repo=None, obj_id=-1, orig=None):
        """
        This is an error in accessing an object in the repo
        Args:
            repo (GangaRepository): The repository the error happened in
            obj_id (int): The key of the object in the objects dict where this happened
            orig (exception): The original exception
        """
        super(InaccessibleObjectError, self).__init__("Inaccessible Object: %s" % obj_id)
        self.repo = repo
        self.obj_id = obj_id
        self.orig = orig

    def __str__(self):
        from GangaCore.GPIDev.Base.Proxy import getName
        return "Repository '%s' object #%s is not accessible because of an %s: %s" % \
               (self.repo.registry.name, self.obj_id, getName(self.orig), str(self.orig))


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
        from GangaCore.Utility.logging import getLogger
        logger = getLogger()
        logger.error("A severe error occurred in the Repository '%s': %s" % (repo.registry.name, what))
        logger.error('If you believe the problem has been solved, type "reactivate()" to re-enable ')
        try:
            from GangaCore.Core.InternalServices.Coordinator import disableInternalServices
            disableInternalServices()
            from GangaCore.Core.GangaThread.WorkerThreads import shutDownQueues
            shutDownQueues()
            logger.error("Shutting Down Repository_runtime")
            from GangaCore.Runtime import Repository_runtime
            Repository_runtime.shutdown()
        except:
            logger.error("Unable to disable Internal services, they may have already been disabled!")

class CredentialsError(GangaException):
    """
    Base class for credential-related errors
    """


class CredentialRenewalError(CredentialsError):
    """
    There was some problem with renewing a credential
    """


class InvalidCredentialError(CredentialsError):
    """
    The credential is invalid for some reason
    """


class ExpiredCredentialError(InvalidCredentialError):
    """
    The credential has expired
    """

class GangaKeyError(GangaException, KeyError):
    """
    Class used for known Ganga-related KeyError exception (generally to do with Credential Store) that will consequently
    not generate a traceback for the user.

    TODO: Note that currently KeyError is checked for in the code - this should be shifted to GangaKeyError at the
    earliest oppurtunity!
    """

    def __init__(self, *args, **kwds):
        super(GangaException, self).__init__(args)
        KeyError.__init__(self, *args)
        self.kwds = kwds

class GangaTypeError(GangaException, TypeError):
    """
    Class analogous to GangaKeyError. This class wraps TypeError so that users are prevented from seeing stack traces from known good exceptions thrown in Ganga code.
    """

    def __init__(self, *args, **kwds):
        super(GangaException, self).__init__(args)
        TypeError.__init__(self, *args)
        self.kwds = kwds

