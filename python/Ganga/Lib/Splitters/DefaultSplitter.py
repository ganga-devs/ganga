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
    ## Don't think we need to configure this
    _schema = Schema(Version(1, 0), {})

    def _checkset_values(self, value):
        self._checksetNestedLists(value)
        # TODO: here we may implement advanced validation of type against the
        # type of selected attribute
        pass

    def split(self, job):

        subjobs = []

        sj = self.createSubjob(job)

        subjobs.append(sj)

        return subjobs

