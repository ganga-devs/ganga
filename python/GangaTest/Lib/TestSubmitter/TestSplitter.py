###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestSplitter.py,v 1.1 2008-07-17 16:41:36 moscicki Exp $
###############################################################################

from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *

class TestSplitter(ISplitter):
    """ Splitting to different backends...
    """    
    _name = "TestSplitter"
    _schema = Schema(Version(1,0), {
        'backs' : ComponentItem('backends',defvalue=[],sequence=1,doc='a list of Backend objects'),
        'fail' : SimpleItem(defvalue='',doc='Define the artificial runtime failures: "exception"'),
        } )

    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        subjobs = []
        if self.fail == 'exception':
            x = 'triggered failure during splitting'
            raise Exception(x)
        for b in self.backs:
            j = Job()
            j.copyFrom(job)
            j.backend = b
            subjobs.append(j)
        return subjobs

                
#
#
# $Log: not supported by cvs2svn $
# Revision 1.1.18.1  2007/12/13 16:58:49  moscicki
# adaptation for 5.0
#
# Revision 1.1  2006/07/12 13:09:35  moscicki
# TestSplitter
#
# Revision 1.1  2006/06/21 11:27:29  moscicki
# splitters moved to the new location
#
#
#
