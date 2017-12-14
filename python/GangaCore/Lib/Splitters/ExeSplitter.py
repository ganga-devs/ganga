##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ExeSplitter.py,v 1.1 2008-07-17 16:40:59 moscicki Exp $
##########################################################################

from GangaCore.GPIDev.Schema import Schema, Version, ComponentItem
from GangaCore.GPIDev.Adapters.ISplitter import ISplitter


class ExeSplitter(ISplitter):

    """ Split executable applications (OBSOLETE).

    This splitter allows the creation of subjobs where each subjob has a different Executable application.
    This splitter is OBSOLETED use GenericSplitter or ArgSplitter instead.
    """
    _name = "ExeSplitter"
    _schema = Schema(Version(1, 0), {
        'apps': ComponentItem('applications', defvalue=[], sequence=1, doc='a list of Executable app objects')
    })

    def split(self, job):
        subjobs = []
        for a in self.apps:
            # for each subjob make a full copy of the master job
            j = self.createSubjob(job)
            j.application = a
            if not a.exe:
                j.application.exe = job.application.exe
            subjobs.append(j)
        return subjobs


#
#
# $Log: not supported by cvs2svn $
# Revision 1.3  2007/07/10 13:08:32  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.2  2006/08/24 16:52:10  moscicki
# splitter.createJob()
#
# Revision 1.1  2006/06/21 11:27:29  moscicki
# splitters moved to the new location
#
#
#
