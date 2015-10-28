#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
from GangaLHCb.Lib.LHCbDataset.BKQuery import BKQuery, BKQueryDict
from Ganga.GPIDev.Schema import Schema, Version, ComponentItem, SimpleItem

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class BKTestQuery(BKQuery):
    ##     schema = {}
    ##     docstr = 'Bookkeeping query path (type dependent)'
    ##     schema['path'] = SimpleItem(defvalue='' ,doc=docstr)
    ##     docstr = 'Start date string yyyy-mm-dd (only works for type="RunsByDate")'
    ##     schema['startDate'] = SimpleItem(defvalue='' ,doc=docstr)
    ##     docstr = 'End date string yyyy-mm-dd (only works for type="RunsByDate")'
    ##     schema['endDate'] = SimpleItem(defvalue='' ,doc=docstr)
    ##     docstr = 'Data quality flag (string or list of strings).'
    # schema['dqflag'] = SimpleItem(defvalue='All',typelist=['str','list'],
    # doc=docstr)
    ##     docstr = 'Type of query (Path, RunsByDate, Run, Production)'
    ##     schema['type'] = SimpleItem(defvalue='Path',doc=docstr)
    # docstr = 'Selection criteria: Runs, ProcessedRuns, NotProcessed (only \
    # works for type="RunsByDate")'
    ##     schema['selection'] = SimpleItem(defvalue='',doc=docstr)
    _schema = BKQuery._schema.inherit_copy()
    _schema.datadict['dataset'] = ComponentItem(
        'datasets', defvalue=None, optional=1, load_default=False, doc='dataset', hidden=0)
    _schema.datadict['fulldataset'] = ComponentItem(
        'datasets', defvalue=None, optional=1, load_default=False, doc='dataset', hidden=1)
    _schema.datadict['fulldatasetptr'] = SimpleItem(
        defvalue=0, optional=0, load_default=True, doc='dataset position pointer', hidden=1, typeList=['int'])
    _schema.datadict['filesToRelease'] = SimpleItem(
        defvalue=3, optional=0, load_default=True, doc='number of files to release at a time', hidden=0, typeList=['int'])
    _category = 'query'
    _name = "BKTestQuery"
    _exportmethods = BKQuery._exportmethods
    _exportmethods += ['removeData']

    def getDataset(self):
        if self.fulldataset is None:
            self.fulldataset = LHCbDataset(super(BKTestQuery, self).getDataset().files)
        if self.dataset is None:
            self.dataset = LHCbDataset(self.fulldataset.files[:self.filesToRelease])
            self.fulldatasetptr = self.filesToRelease
        else:
            self.dataset.files += self.fulldataset.files[
                self.fulldatasetptr:self.fulldatasetptr + self.filesToRelease]
            self.fulldatasetptr += self.filesToRelease
        return self.dataset

    def removeData(self):
        if len(self.dataset):
            del self.dataset.files[0]


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
