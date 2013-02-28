from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from GangaCMS.Lib.Utils import SplitterError
from xml.dom.minidom import parse


class CRABSplitter(ISplitter):
    """Splitter object for CRAB jobs."""
    schemadic = {}
    schemadic['maxevents'] = SimpleItem(defvalue=None,
                                        typelist=['type(None)', 'int'],
                                        doc='Maximum number of events/task')
    schemadic['inputfiles'] = SimpleItem(defvalue=None,
                                         typelist=['type(None)', 'str'],
                                         doc='Number of input files')
    schemadic['skipevents'] = SimpleItem(defvalue=None,
                                         typelist=['type(None)', 'int'],
                                         doc='Offset for the events')
    _name = 'CRABSplitter'
    _schema = Schema(Version(1, 0), schemadic)

    def parseArguments(self, path):
        """Gets some job arguments from the FJR."""
        splittingData = []
        for job in parse(path).getElementsByTagName("Job"):
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
        for maxevents, inputfiles, skipevents in splittingData:
            j = self.createSubjob(job)
            j.master = job
            j.application = job.application
            j.inputdata = job.inputdata
            j.backend = job.backend

            splitter = CRABSplitter()
            splitter.maxevents = maxevents
            splitter.inputfiles = inputfiles
            splitter.skipevents = skipevents
            j.splitter = splitter
            subjobs.append(j)
        return subjobs
