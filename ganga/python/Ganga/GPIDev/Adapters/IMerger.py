################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: IMerger.py,v 1.1 2008-07-17 16:40:52 moscicki Exp $
################################################################################
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

class MergerError(GangaException):
    def __init__(self,x): Exception.__init__(self,x)

class IMerger(GangaObject):
    """
    """
    _schema =  Schema(Version(0,0), {})
    _category = 'mergers'
    _hidden = 1

    def merge(self, sum_outputdir, subjobs, **options ):
        """
        Merge the output of subjobs into the sum_outputdir.
        The options (keyword arguments) are merger-implementation specific and should be defined in the derived classes.
        """

        raise NotImplementedError

    def validatedMerge(self,job):
        """ Some info """

        self.merge(job, sum_outputdir, subjobs)

        return None
    
