from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.Job import Job


class TestSplitter(ISplitter):

    '''Split a job into n subjobs
    '''
    _name = "TestSplitter"
    docstr = "number of subjobs"
    _schema = Schema(Version(1, 0),
                     {'numJobs': SimpleItem(defvalue=10, doc=docstr)})

    def split(self, job):
        subjobs = []

        for i in range(self.numJobs):
            j = Job()
            j.copyFrom(job)
            j.splitter = None
            j.merger = None
            j.inputsandbox = []  # master added automatically
            subjobs.append(j)

        return subjobs
