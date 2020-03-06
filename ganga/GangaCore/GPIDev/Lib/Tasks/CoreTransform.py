
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Lib.Tasks.ITransform import ITransform
from GangaCore.GPIDev.Lib.Tasks.CoreUnit import CoreUnit
from GangaCore.GPIDev.Lib.Dataset.GangaDataset import GangaDataset
from GangaCore.GPIDev.Lib.Job.Job import Job
from GangaCore.GPIDev.Base.Proxy import stripProxy, getName, isType
from GangaCore.Lib.Splitters.GenericSplitter import GenericSplitter
import copy
import re

logger = getLogger()

class CoreTransform(ITransform):
    _schema = Schema(Version(1, 0), dict(list(ITransform._schema.datadict.items()) + list({
        'unit_splitter': ComponentItem('splitters', defvalue=None, optional=1, load_default=False, doc='Splitter to be used to create the units'),
        'chaindata_as_inputfiles': SimpleItem(defvalue=False, doc="Treat the inputdata as inputfiles, i.e. copy the inputdata to the WN"),
        'files_per_unit': SimpleItem(defvalue=-1, doc="Number of files per unit if possible. Set to -1 to just create a unit per input dataset"),
        'fields_to_copy': SimpleItem(defvalue=[], typelist=[str], sequence=1, doc='A list of fields that should be copied when creating units, e.g. application, inputfiles. Empty (default) implies all fields are copied unless the GeenricSplitter is used '),
    }.items())))

    _category = 'transforms'
    _name = 'CoreTransform'
    _exportmethods = ITransform._exportmethods + []

    def __init__(self):
        super(CoreTransform, self).__init__()

    def createUnits(self):
        """Create new units if required given the inputdata"""

        # call parent for chaining
        super(CoreTransform, self).createUnits()

        # Use the given splitter to create the unit definitions
        if len(self.units) > 0:
            # already have units so return
            return

        if self.unit_splitter is None and len(self.inputdata) == 0:
            raise ApplicationConfigurationError("No unit splitter or InputData provided for CoreTransform unit creation, Transform %d (%s)" %
                                                (self.getID(), self.name))

        # -----------------------------------------------------------------
        # split over unit_splitter by preference
        if self.unit_splitter:

            # create a dummy job, assign everything and then call the split
            j = Job()
            j.backend = self.backend.clone()
            j.application = self.application.clone()

            if self.inputdata:
                j.inputdata = self.inputdata.clone()

            subjobs = self.unit_splitter.split(j)

            if len(subjobs) == 0:
                raise ApplicationConfigurationError("Unit splitter gave no subjobs after split for CoreTransform unit creation, Transform %d (%s)" %
                                                    (self.getID(), self.name))

            # only copy the appropriate elements
            fields = []
            if len(self.fields_to_copy) > 0:
                fields = self.fields_to_copy
            elif isType(self.unit_splitter, GenericSplitter):
                if self.unit_splitter.attribute != "":
                    fields = [self.unit_splitter.attribute.split(".")[0]]
                else:
                    for attr in self.unit_splitter.multi_attrs.keys():
                        fields.append(attr.split(".")[0])

            # now create the units from these jobs
            for sj in subjobs:
                unit = CoreUnit()

                for attr in fields:
                    setattr(unit, attr, copy.deepcopy(getattr(sj, attr)))

                self.addUnitToTRF(unit)

        # -----------------------------------------------------------------
        # otherwise split on inputdata
        elif len(self.inputdata) > 0:

            if self.files_per_unit > 0:

                # combine all files and split accorindgly
                filelist = []
                for ds in self.inputdata:

                    if isType(ds, GangaDataset):
                        for f in ds.files:
                            if f.containsWildcards():
                                # we have a wildcard so grab the subfiles
                                for sf in f.getSubFiles(process_wildcards=True):
                                    filelist.append(sf)
                            else:
                                # no wildcards so just add the file
                                filelist.append(f)
                    else:
                        logger.warning("Dataset '%s' doesn't support files" % getName(ds))

                # create DSs and units for this list of files
                fid = 0
                while fid < len(filelist):
                    unit = CoreUnit()
                    unit.name = "Unit %d" % len(self.units)
                    unit.inputdata = GangaDataset(
                        files=filelist[fid:fid + self.files_per_unit])

                    fid += self.files_per_unit

                    self.addUnitToTRF(unit)

            else:
                # just produce one unit per dataset
                for ds in self.inputdata:

                    # avoid splitting over chain inputs
                    if isType(ds, TaskChainInput):
                        continue

                    unit = CoreUnit()
                    unit.name = "Unit %d" % len(self.units)
                    unit.inputdata = copy.deepcopy(ds)
                    self.addUnitToTRF(unit)

    def createChainUnit(self, parent_units, use_copy_output=True):
        """Create an output unit given this output data"""

        # check parent units/jobs are complete
        if not self.checkUnitsAreCompleted(parent_units):
            return None

        # get the include/exclude masks
        incl_pat_list, excl_pat_list = self.getChainInclExclMasks(parent_units)

        # go over the output files and transfer to input data
        flist = []
        for sj in self.getParentUnitJobs(parent_units):
            for f in sj.outputfiles:
                temp_flist = stripProxy(f).getSubFiles() if len(stripProxy(f).getSubFiles()) > 0 else [stripProxy(f)]
                for f2 in temp_flist:
                    if len(incl_pat_list) > 0:
                        for pat in incl_pat_list:
                            if re.search(pat, f2.namePattern):
                                flist.append(f2)
                    else:
                        flist.append(f2)

                    for pat in excl_pat_list:
                        if re.search(pat, f2.namePattern):
                            flist.remove(f2)

        # now create the unit with a GangaDataset
        unit = CoreUnit()
        unit.name = "Unit %d" % len(self.units)
        unit.inputdata = GangaDataset(files=flist)

        return unit
