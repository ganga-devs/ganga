
from Ganga.GPIDev.Lib.Tasks.TaskApplication import taskify
from GangaAtlas.Lib.AthenaMC.AthenaMC import AthenaMC, AthenaMCSplitterJob
from GangaAtlas.Lib.Athena.Athena import Athena
AthenaTask = taskify(Athena,"AthenaTask")
AthenaMCTask = taskify(AthenaMC,"AthenaMCTask")
AthenaMCTaskSplitterJob = taskify(AthenaMCSplitterJob,"AthenaMCTaskSplitterJob")
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
class AnaTaskSplitterJob(ISplitter):
    """AnaTask handler for job splitting"""
    _name = "AnaTaskSplitterJob"
    _category = "splitters"
    _schema = Schema(Version(1,0), {
        'subjobs'           : SimpleItem(defvalue=[],sequence=1, doc="List of subjobs", typelist=["int"]),
        'numevtsperjob'     : SimpleItem(defvalue=0, doc='Number of events per subjob'),
        'numevtsperfile'    : SimpleItem(defvalue=0,doc='Maximum number of events in a file of input dataset')
    } )
    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        logger.debug("AnaTaskSplitterJob split called")
        sjl = []
        transform = stripProxy(job.application.getTransform())
        transform.setAppStatus(job.application, "removed")
        # Do the splitting
        for sj in self.subjobs:
            j = Job()
            j.inputdata = transform.partitions_data[sj-1]
            j.outputdata = job.outputdata
            j.application = job.application
            j.application.atlas_environment.append("OUTPUT_FILE_NUMBER=%i" % sj)
            j.backend = job.backend
            if transform.partitions_sites:
                if hasattr(j.backend.requirements, 'sites'):                
                    j.backend.requirements.sites = transform.partitions_sites[sj-1]                    
                else:
                    j.backend.site = transform.partitions_sites[sj-1]

            j.inputsandbox = job.inputsandbox
            j.outputsandbox = job.outputsandbox
            sjl.append(j)
            # Task handling
            j.application.tasks_id = job.application.tasks_id
            j.application.id = transform.getNewAppID(sj)
             #transform.setAppStatus(j.application, "submitting")
        if not job.application.tasks_id.startswith("00"):
            job.application.tasks_id = "00:%s" % job.application.tasks_id
        return sjl
    
from Ganga.GPIDev.Lib.Tasks.TaskApplication import task_map
task_map["Athena"] = AthenaTask
task_map["AthenaMC"] = AthenaMCTask
