###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AMAAthenaTask.py,v 1.32 2009-05-29 13:27:14 dvanders Exp $
###############################################################################
# Customized GangaTask for AMAAthena analysis
#
# ATLAS/ARDA

import re

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base.Proxy import stripProxy
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from GangaAtlas.Lib.Tasks.Transform import Transform
from GangaAtlas.Lib.Tasks.AnaTransform import AnaTransform
from GangaAtlas.Lib.Tasks.TaskApplication import taskify
from GangaAtlas.Lib.AMAAthena.AMAAthena import AMAAthena

AMAAthenaTask = taskify(AMAAthena,"AMAAthenaTask")

class AMAAthenaTaskSplitterJob(ISplitter):
    """AMAAthenaTask handler for job splitting"""
    _name = "AMAAthenaTaskSplitterJob"
    _category = "splitters"
    _schema = Schema(Version(1,0), {
        'subjobs'           : SimpleItem(defvalue=[],sequence=1, doc="List of subjobs", typelist=["int"]),
    } )
    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        logger.debug("AnaTaskSplitterJob split called")
        sjl = []

        ## get the corresponding transform
        transform = stripProxy(job.application.getTransform())
        transform.setAppStatus(job.application, "removed")

        ## get the corresponding task from the given transform
        task = transform._getParent()

        ## get the index of transform in the task
        id = task.transforms.index(transform)

        # Do the splitting
        for sj in self.subjobs:

            j = Job()

            j.name = "T%i.TF%i.P%i" % (task.id, id, sj)

            j.inputdata = transform.partitions_data[sj-1]
            j.outputdata = job.outputdata
            j.application = job.application
            j.application.atlas_environment.append("OUTPUT_FILE_NUMBER=%i" % sj)
            j.backend = job.backend
            if transform.partitions_sites:
                j.backend.requirements.sites = transform.partitions_sites[sj-1]
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

class AMAAthenaTransform(AnaTransform):

    _schema = Schema(Version(1,0), dict(AnaTransform._schema.datadict.items()))
    _category = 'transforms'
    _name = 'AMAAthenaTransform'
    _exportmethods = Transform._exportmethods

    def initialize(self):
        super(AnaTransform, self).initialize()
        self.application = AMAAthenaTask()
        self.name = 'AMAAthena Transform'

    ## every time the partions are given, only one Ganga master job is created.
    ## The subjobs are created by splitter object therefore the partitions are
    ## actually given to the j.splitter.subjobs
    def getJobsForPartitions(self, partitions):

        task = self._getParent()

        id = task.transforms.index(self)

        j = self.createNewJob(partitions[0])

        ## the name has to be specified carefully, because this is going to be
        ## used by Ganga/AMAAthena to set AMA_SAMPLE_NAME environment variable
        j.name = "T%i.TF%i.P%i" % (task.id, id, partitions[0])

        if len(partitions) > 1:
            j.splitter = AMAAthenaTaskSplitterJob()
            j.splitter.subjobs = partitions
        j.inputdata = self.partitions_data[partitions[0]-1]
        if self.partitions_sites:
            j.backend.requirements.sites = self.partitions_sites[partitions[0]-1]
        j.outputdata = self.outputdata
        if j.outputdata.datasetname:
            today = time.strftime("%Y%m%d",time.localtime())
            j.outputdata.datasetname = "%s.%i.%s" % (j.outputdata.datasetname, j.id, today)
        return [j]

    ## Internal methods
    def checkCompletedApp(self, app):
        j = app._getParent()
        for f in j.outputdata.output:
           if "ama_summary" in f:
              return True
        logger.error("Job %s has not produced %s file, only: %s" % (j.id, "ama_summary", j.outputdata.output))
        return False
