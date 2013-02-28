from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from GangaCMS.Lib.Utils import SplitterError
from xml.dom.minidom import parse


class CRABSplitter(ISplitter):
    """Splitter object for CRAB jobs."""
    schemadic = {}
    schemadic['maxevents'] = SimpleItem(defvalue=None, typelist=['type(None)', 'int'], doc='')
    schemadic['inputfiles'] = SimpleItem(defvalue=None, typelist=['type(None)', 'str'], doc='')
    schemadic['skipevents'] = SimpleItem(defvalue=None, typelist=['type(None)', 'int'], doc='')
    _name = 'CRABSplitter'
    _schema = Schema(Version(1, 0), schemadic)

    def parseArguments(self, path):
        """Gets some job arguments from the FJR."""
        jobs = parse(path).getElementsByTagName("Job")
        splittingData = []

        for job in jobs:
            splittingData.append([job.getAttribute("MaxEvents"),
                                  job.getAttribute("InputFiles"),
                                  job.getAttribute("SkipEvents")])
        return splittingData

    def split(self, job):
        """Main splitter for the job."""
        try:
            splittingData = self.parseArguments('%sshare/arguments.xml' %
                                                job.inputdata.ui_working_dir)
        except IOError, e:
            raise SplitterError(e)

        subjobs = []
        for index in range(len(splittingData)):
            j = self.createSubjob(job)
            j.master = job
            j.application = job.application
            j.inputdata = job.inputdata
            j.backend = job.backend

            splitter = CRABSplitter()
            splitter.maxevents = splittingData[index][0]
            splitter.inputfiles = splittingData[index][1]
            splitter.skipevents = splittingData[index][2]
            j.splitter = splitter
            subjobs.append(j)
        return subjobs
