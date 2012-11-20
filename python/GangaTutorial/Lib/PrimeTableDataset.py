################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PrimeTableDataset.py,v 1.1 2008-07-17 16:41:37 moscicki Exp $
################################################################################

from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig
from Ganga.GPIDev.Lib.File import File
from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.Utility.logging import getLogger

class PrimeTableDataset(Dataset):
    """Prime number lookup table definition."""
    
    _schema = Schema(Version(1,0), {
        'table_id_lower': SimpleItem(defvalue=1, doc='The lower bound id of the lookup tables (inclusive).'),
        'table_id_upper': SimpleItem(defvalue=1, doc='The upper bound id of the lookup tables (inclusive).'),
        'table_id_max': SimpleItem(defvalue=15, hidden=1, doc='The upper bound id of the lookup tables (inclusive).'),
        'table_location': SimpleItem(defvalue='http://cern.ch/diane/download/primes', doc='The location of the lookup tables.')
        })
    
    _category = 'datasets'
    _name = 'PrimeTableDataset'
    _exportmethods = ['get_dataset']

    def __init__(self):
        super(PrimeTableDataset,self).__init__()

    def get_dataset(self):
        from urlparse import urlparse
        tables = []

        ## switching lower and upper if different order is given
        if self.table_id_lower > self.table_id_upper:
            tmp = self.table_id_lower
            self.table_id_lower = self.table_id_upper
            self.table_id_upper = tmp

        ## restrict maximum table id to self.table_id_max 
        if self.table_id_upper > self.table_id_max:
            logger.warning('table_id_upper is restricted to up to %d' % self.table_id_max)
            self.table_id_upper = self.table_id_max

        for i in range(self.table_id_lower,self.table_id_upper+1):
            location = '%s/primes%d.zip' % (self.table_location,i)
            url = urlparse(location)
            if url[0] in ['file','']:
                # create new File object if the location is local  
                tables.append(File(location))
            else:    
                tables.append(location)
        return tables

logger = getLogger()
