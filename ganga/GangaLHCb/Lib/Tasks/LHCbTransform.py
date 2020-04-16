from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from GangaCore.Core.exceptions import GangaException
from GangaCore.GPIDev.Lib.Tasks.common import getJobByID
from GangaCore.GPIDev.Lib.Tasks.ITransform import ITransform
from GangaCore.GPIDev.Lib.Job.Job import JobError
from GangaCore.GPIDev.Lib.Registry.JobRegistry import JobRegistrySlice, JobRegistrySliceProxy
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.Core.exceptions import GangaAttributeError
from GangaLHCb.Lib.Tasks.LHCbUnit import LHCbUnit
from GangaCore.GPIDev.Base.Proxy import isType
from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaCore.GPIDev.Lib.File.LocalFile import LocalFile
from GangaCore.GPIDev.Lib.File.MassStorageFile import MassStorageFile
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Base.Proxy import stripProxy

logger = getLogger()

class LHCbTransform(ITransform):
    _schema = Schema(Version(1, 0), dict(list(ITransform._schema.datadict.items()) + list({
        'files_per_unit': SimpleItem(defvalue=-1, doc='Maximum number of files to assign to each unit from a given input dataset. If < 1, use all files.', typelist=["int"]),
        'splitter': ComponentItem('splitters', defvalue=None, optional=1, load_default=False, doc='Splitter to be used for units'),
        'queries': ComponentItem('query', defvalue=[], sequence=1, protected=1, optional=1, load_default=False, doc='Queries managed by this Transform'),
        'delete_chain_input': SimpleItem(defvalue=False, doc='Delete the Dirac input files/data after completion of each unit', typelist=["bool"]),
        'mc_num_units': SimpleItem(defvalue=0, doc="No. of units to create for MC generation"),

    }.items())))

    _category = 'transforms'
    _name = 'LHCbTransform'
    _exportmethods = ITransform._exportmethods + ['updateQuery', 'addQuery', 'removeUnusedData', 'cleanTransform']

    def __init__(self):
        super(LHCbTransform, self).__init__()

        # generally no delay neededd
        self.chain_delay = 0

    def addQuery(self, bk):
        """Add a BK query to this transform"""
        # Check if the BKQuery input is correct and append/update
        if not isType(bk, BKQuery):
            raise GangaAttributeError(
                None, 'LHCbTransform expects a BKQuery object passed to the addQuery method')

        # check we don't already have inputdata
        if len(self.queries) == 0 and len(self.inputdata) > 0:
            logger.error(
                "Cannot add both input data and BK queries. Input Data already present.")
            return

        # add the query and update the input data
        self.queries.append(bk)
        self.updateQuery()

    def addInputQuery(self, inDS):
        """Add the given input dataset to the list but only if BK queries aren't given"""
        if len(self.queries) > 0:
            logger.error(
                "Cannot add both input data and BK queries. Query already given")
            return

        super(LHCbTransform, self).addInputQuery(inDS)

    def cleanTransform(self):
        """Remove unused data and then unused jobs"""
        self.removeUnusedData()
        self.removeUnusedJobs()

    def removeUnusedData(self):
        """Remove any output data from orphaned jobs"""
        for unit in self.units:
            for jid in unit.prev_job_ids:
                try:
                    logger.warning("Removing data from job '%d'..." % jid)
                    job = getJobByID(jid)

                    jlist = []
                    if len(job.subjobs) > 0:
                        jlist = job.subjobs
                    else:
                        jlist = [job]

                    for sj in jlist:
                        for f in sj.outputfiles:
                            if isType(f, DiracFile) == "DiracFile" and f.lfn:
                                f.remove()
                except:
                    logger.error("Problem deleting data for job '%d'" % jid)
                    pass

    def createUnits(self):
        """Create new units if required given the inputdata"""

        # call parent for chaining
        super(LHCbTransform, self).createUnits()

        if len(self.inputdata) > 0:

            # check for conflicting input
            if self.mc_num_units > 0:
                logger.warning("Inputdata specified - MC Event info ignored")

            # loop over input data and see if we need to create any more units
            import copy
            for id, inds in enumerate(self.inputdata):

                if not isType(inds, LHCbDataset):
                    continue

                # go over the units and see what files have been assigned
                assigned_data = LHCbDataset()
                for unit in self.units:

                    if unit.input_datset_index != id:
                        continue

                    assigned_data.files += unit.inputdata.files

                # any new files
                new_data = LHCbDataset(files=self.inputdata[id].difference(assigned_data).files)

                if len(new_data.files) == 0:
                    continue

                # Create units for these files
                step = self.files_per_unit
                if step <= 0:
                    step = len(new_data.files)

                for num in range(0, len(new_data.files), step):
                    unit = LHCbUnit()
                    unit.name = "Unit %d" % len(self.units)
                    unit.input_datset_index = id
                    self.addUnitToTRF(unit)
                    unit.inputdata = copy.deepcopy(self.inputdata[id])
                    unit.inputdata.files = []
                    unit.inputdata.files += new_data.files[num:num+step]

        elif self.mc_num_units > 0:
            if len(self.units) == 0:
                # check for appropriate splitter
                from GangaLHCb.Lib.Splitters.GaussSplitter import GaussSplitter
                if not self.splitter or isType(self.splitter, GaussSplitter):
                    logger.warning("No GaussSplitter specified - first event info ignored")

                # create units for MC generation
                for i in range(0, self.mc_num_units):
                    unit = LHCbUnit()
                    unit.name = "Unit %d" % len(self.units)
                    self.addUnitToTRF(unit)
        else:
            import traceback
            traceback.print_stack()
            logger.error("Please specify either inputdata or MC info for unit generation")

    def createChainUnit(self, parent_units, use_copy_output=True):
        """Create an output unit given this output data"""

        # we need a parent job that has completed to get the output files
        incl_pat_list = []
        excl_pat_list = []
        for parent in parent_units:
            if len(parent.active_job_ids) == 0 or parent.status != "completed":
                return None

            for inds in self.inputdata:
                from GangaCore.GPIDev.Lib.Tasks.TaskChainInput import TaskChainInput
                if isType(inds, TaskChainInput) and inds.input_trf_id == parent._getParent().getID():
                    incl_pat_list += inds.include_file_mask
                    excl_pat_list += inds.exclude_file_mask

        # go over the output files and copy the appropriates over as input
        # files
        flist = []
        flist_local = []
        import re
        for parent in parent_units:
            job = getJobByID(parent.active_job_ids[0])
            if job.subjobs:
                job_list = job.subjobs
            else:
                job_list = [job]

            for sj in job_list:
                for f in sj.outputfiles:

                    # match any dirac files that are allowed in the file mask
                    if isType(f, DiracFile):
                        if len(incl_pat_list) > 0:
                            for pat in incl_pat_list:
                                if re.search(pat, f.lfn):
                                    flist.append("LFN:" + f.lfn)
                        else:
                            flist.append("LFN:" + f.lfn)

                        if len(excl_pat_list) > 0:
                            for pat in excl_pat_list:
                                if re.search(pat, f.lfn) and "LFN:" + f.lfn in flist:
                                    flist.remove("LFN:" + f.lfn)
                    elif isType(f, LocalFile) or isType(f, MassStorageFile):
                        if len(incl_pat_list) > 0:
                            for pat in incl_pat_list:
                                if re.search(pat, f.namePattern):
                                    flist_local.append(f)
                        else:
                            flist_local.append(f)

                        if len(excl_pat_list) > 0:
                            for pat in excl_pat_list:
                                if re.search(pat, f.namePattern) and f.location[0] in flist_local:
                                    flist_local.remove(f.location[0])


        # just do one unit that uses all data
        unit = LHCbUnit()
        unit.name = "Unit %d" % len(self.units)
        if len(flist_local)==0:
            unit.inputdata = LHCbDataset(files=[DiracFile(lfn=f) for f in flist])
        elif len(flist_local)!=0 and len(flist)!=0:
            logger.warning("Found both DiracFile and LocalFile to copy job input. Only taking DiracFile")
            unit.inputdata = LHCbDataset(files=[DiracFile(lfn=f) for f in flist])
        else:
            unit.inputdata = LHCbDataset(files = [f for f in flist_local])

        return unit

    def updateQuery(self, resubmit=False):
        """Update the dataset information of the transforms. This will
        include any new data in the processing or re-run jobs that have data which
        has been removed."""
        if len(self.queries) == 0:
            raise GangaException(
                None, 'Cannot call updateQuery() on an LHCbTransform without any queries')

        if self._getParent() is not None:
            logger.info('Retrieving latest bookkeeping information for transform %i:%i, please wait...' % (
                self._getParent().id, self.getID()))
        else:
            logger.info(
                'Retrieving latest bookkeeping information for transform, please wait...')

        # check we have an input DS per BK Query
        while len(self.queries) > len(self.inputdata):
            self.inputdata.append(LHCbDataset())

        # loop over the queries and add fill file lists
        for id, query in enumerate(self.queries):

            # Get the latest dataset
            latest_dataset = stripProxy(query.getDataset())

            # Compare to previous inputdata, get new and removed
            logger.info(
                'Checking for new and removed data for query %d, please wait...' % self.queries.index(query))
            dead_data = LHCbDataset()
            new_data = LHCbDataset()

            # loop over the old data and compare
            new_data.files += latest_dataset.difference(
                self.inputdata[id]).files
            dead_data.files += self.inputdata[
                id].difference(latest_dataset).files

            # for dead data, find then kill/remove any associated jobs
            # loop over units and check any associated with this DS
            # TODO: Follow through chained tasks
            for unit in self.units:
                # associted unit
                if unit.input_datset_index != id:
                    continue

                # find the job
                if len(unit.active_job_ids) == 0:
                    continue

                # check the data
                for f in dead_data.files:
                    if f in unit.inputdata.files:

                        # kill the job
                        job = getJobByID(unit.active_job_ids[0])
                        if job.status in ['submitted', 'running']:
                            job.kill()

                        # forget the job
                        unit.prev_job_ids.append(unit.active_job_ids[0])
                        unit.active_job_ids = []
                        break

            # in any case, now just set the DS files to the new set
            self.inputdata[id].files = []
            self.inputdata[id].files = latest_dataset.files
