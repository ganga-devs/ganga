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

    # set outputdir for auto merge policy flag
    # the default behaviour (True) is that outputdir is set by runAutoMerge() function in Merger.py module
    # however if this flag is set to False then merge() will be called for auto merge with sum_outputdir set to None
    # thus it is up to the subclass to decide where the output goes in case of auto merge
    set_outputdir_for_automerge = True

    def merge(self, subjobs, sum_outputdir, **options ):
        """
        Merge the output of subjobs into the sum_outputdir.
        The options (keyword arguments) are merger-implementation specific and should be defined in the derived classes.
        """

        raise NotImplementedError

    def validatedMerge(self,job):
        """ Some info """

        self.merge(job, sum_outputdir, subjobs)

        return None
    
