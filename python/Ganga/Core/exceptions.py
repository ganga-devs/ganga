################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: exceptions.py,v 1.2 2008-09-09 14:37:16 moscicki Exp $
################################################################################


class GangaException(Exception):
    """ Markup base class for well-behaved exception that should not print the whole traceback to user's prompt
        Any subclass of this exception is handled by a custom IPython exception handler
        and is printed out in an usable format to iPython prompt
    """

    def __init__(self,*args,**kwds):
        Exception.__init__(self,*args)
        self.kwds = kwds

    def __str__(self):
        """
         String representation of this class
        """
        _str = "%s: " % self.__class__.__name__
        if hasattr(self,'args') and self.args:
            _str +=" %s" % str(self.args)
        if hasattr(self,'kwds') and self.kwds:
            _str +=" %s" % str(self.kwds)
        return _str

class ApplicationConfigurationError(GangaException):
    def __init__(self,excpt,message):
        GangaException.__init__(self,excpt,message)
        self.message = message
        self.excpt = excpt

    def __str__(self):
        if self.excpt:
            e = '(%s:%s)'%(str(type(self.excpt)),str(self.excpt))
        else:
            e = ''
        return "ApplicationConfigurationError: %s %s"%(self.message,e)

class BackendError(GangaException):
    def __init__(self,backend_name,message):
        GangaException.__init__(self,backend_name,message)
        self.backend_name = backend_name
        self.message = message

    def __str__(self):
        return "BackendError: %s (%s backend) "%(self.message,self.backend_name)

    
# Exception raised by the Ganga Repository
class RepositoryError(GangaException):
    """
    For non-bulk operations this exception may contain an original
    exception 'e' raised by the DB client.
    """
    def __init__(self, e = None, msg = None, details = None):
        if msg == None:
            msg = "RepositoryError: %s" % str(e)
        GangaException.__init__(self, msg)
        self.e = e
        self.details = details 
        
    def getOriginalMDError(self):
        return self.e


# Exception raised by the Ganga Repository
class BulkOperationRepositoryError(RepositoryError):
    """
    For bulk operations this exception
    have a non-empty dictionary 'details'
    which contains ids of failed jobs as keys and 'original' exceptions as values.
    """
    def __init__(self, details = None, msg = None):
        if msg == None:
            msg = "RepositoryError: %s"% str(e)
        RepositoryError.__init__(self, msg = msg, details = details)
        if details == None:
            self.details = {}

    def listFailedJobs(self):
        return self.details.keys()

    def getOriginalJobError(self, id):        
        return self.details.get(id)

class IncompleteJobSubmissionError(GangaException):
    def __init__(self,*args):
        GangaException.__init__(self,*args)

class IncompleteKillError(GangaException):
    def __init__(self,*args):
        GangaException.__init__(self,*args)        

class JobManagerError(GangaException):
    def __init__(self,msg):
        self.msg = msg
        GangaException.__init__(self,msg)

    def __str__(self):
        return "JobManagerError: %s" % str(self.msg)

class GangaAttributeError(AttributeError,GangaException):
    def __init__(self,*a,**k): AttributeError.__init__(self,*a,**k)

class GangaValueError(ValueError,GangaException):
    def __init__(self,*a,**k): ValueError.__init__(self,*a,**k)
    
class ProtectedAttributeError(GangaAttributeError):
    'Attribute is read-only and may not be modified by the user (for example job.id)'
    def __init__(self,*a,**k): GangaAttributeError.__init__(self,*a,**k)


class ReadOnlyObjectError(GangaAttributeError):
    'Object cannot be modified (for example job in a submitted state)'
    def __init__(self,*a,**k): GangaAttributeError.__init__(self,*a,**k)


class TypeMismatchError(GangaAttributeError):
    def __init__(self,*a,**k): GangaAttributeError.__init__(self,*a,**k)


class SchemaError(GangaAttributeError):
    def __init__(self,*a,**k): GangaAttributeError.__init__(self,*a,**k)

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
