###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: DefaultSplitter.py,v 1.2 2008-09-09 15:11:35 moscicki Exp $
###############################################################################

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

from Ganga.Utility.logging import getLogger
logger = getLogger()


class DefaultSplitter(ISplitter):

    """
        The DefaultSplitter is assigned to all jobs by default and is intended to provide a single subjob for every job on submit.
        This has been implemented as it potentially simplifies the internal logic in job managment significantly.

        This splitter is not expected to be configurable or to split a dataset based upon any input.
        In order to do this please make use of another splitter.
    """
    _name = "DefaultSplitter"
    ## A dummy value is required to not get a bug in writing the object to an XML repo.
    ## The nature of the problem of writing an empty schema should probably be understood more correctly but is difficult to track down -rcurrie
    _schema = Schema(Version(1, 0), {'dummy_value': SimpleItem(defvalue=1, hidden=1, visitable=0, doc='the number of files per subjob', typelist=[int])})

    def split(self, job):

        subjobs = []

        sj = self.createSubjob(job)

        subjobs.append(sj)

        return subjobs

