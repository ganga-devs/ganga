from Ganga.GPIDev.Base.Proxy import isType
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.GPIDev.Lib.Tasks.IUnit import IUnit
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
from GangaLHCb.Lib.Splitters.SplitByFiles import SplitByFiles
from Ganga.GPIDev.Lib.Tasks.common import makeRegisteredJob, getJobByID
from Ganga.Utility.logging import getLogger

logger = getLogger()

class LHCbUnit(IUnit):
    _schema = Schema(Version(1, 0), dict(IUnit._schema.datadict.items() + {
        'input_datset_index': SimpleItem(defvalue=-1, protected=1, hidden=1, doc='Index of input dataset from parent Transform', typelist=["int"]),
    }.items()))

    _category = 'units'
    _name = 'LHCbUnit'
    _exportmethods = IUnit._exportmethods + []

    def createNewJob(self):
        """Create any jobs required for this unit"""
        import copy
        j = makeRegisteredJob()
        j.backend = self._getParent().backend.clone()
        j.application = self._getParent().application.clone()
        if self.inputdata:
            j.inputdata = self.inputdata.clone()

        j.inputfiles = copy.deepcopy(self._getParent().inputfiles)

        trf = self._getParent()
        task = trf._getParent()
        j.inputsandbox = self._getParent().inputsandbox

        j.outputfiles = copy.deepcopy(self._getParent().outputfiles)
        if len(self._getParent().postprocessors.process_objects) > 0:
            j.postprocessors = copy.deepcopy(
                addProxy(self._getParent()).postprocessors)

        if trf.splitter:
            j.splitter = trf.splitter.clone()

            # change the first event for GaussSplitter
            from GangaLHCb.Lib.Splitters.GaussSplitter import GaussSplitter
            if isType(trf.splitter, GaussSplitter):
                events_per_unit = j.splitter.eventsPerJob * \
                    j.splitter.numberOfJobs
                j.splitter.firstEventNumber += self.getID() * events_per_unit

        else:
            j.splitter = SplitByFiles()

        return j

    def checkMajorResubmit(self, job):
        """check if this job needs to be fully rebrokered or not"""
        return False

    def majorResubmit(self, job):
        """perform a major resubmit/rebroker"""
        super(LHCbUnit, self).majorResubmit(job)

    def reset(self):
        """Reset the unit completely"""
        super(LHCbUnit, self).reset()

    def updateStatus(self, status):
        """Update status hook"""

        # check for input data deletion of chain data
        if status == "completed" and self._getParent().delete_chain_input and len(self.req_units) > 0:

            # the inputdata field *must* be filled from the parent task
            # NOTE: When changing to inputfiles, will probably need to check
            # for any specified in trf.inputfiles

            # check that the parent replicas have been copied by checking
            # backend status == Done
            job_list = []
            for req_unit in self.req_units:
                trf = self._getParent()._getParent().transforms[
                    int(req_unit.split(":")[0])]
                req_unit_id = req_unit.split(":")[1]

                if req_unit_id != "ALL":
                    unit = trf.units[int(req_unit_id)]
                    job_list.append(getJobByID(unit.active_job_ids[0]))
                else:
                    for unit in trf.units:
                        job_list.append(getJobByID(unit.active_job_ids[0]))

            for j in job_list:
                if j.subjobs:
                    for sj in j.subjobs:
                        if sj.backend.status != "Done":
                            return
                else:
                    if j.backend.status != "Done":
                        return

            job = getJobByID(self.active_job_ids[0])
            for f in job.inputdata.files:
                # check for an lfn
                if hasattr(f, "lfn"):
                    fname = f.lfn
                else:
                    fname = f.namePattern

                logger.warning(
                    "Removing chain inputdata file '%s'..." % fname)
                f.remove()

        super(LHCbUnit, self).updateStatus(status)

    def checkForSubmission(self):
        """Additional checks for unit submission"""

        # call the base class
        if not super(LHCbUnit, self).checkForSubmission():
            return False

        return True
