#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
''' Splitter for DIRAC jobs. '''

from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.ISplitter import ISplitter, SplittingError
import Ganga.Utility.logging
from GangaLHCb.Lib.Splitters.SplitByFiles import SplitByFiles
from Dirac import Dirac
from DiracUtils import *

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracSplitter(SplitByFiles):
    """Query the LFC, via Dirac, to find optimal data file grouping.

    This Splitter will query the Logical File Catalog (LFC) to find
    at which sites a particular file is stored. Subjobs will be created
    so that all the data required for each subjob is stored in
    at least one common location. This prevents the submission of jobs that
    are unrunnable due to data availability.
    """

    _name = 'DiracSplitter'
    _schema = SplitByFiles._schema.inherit_copy()

    def __init__(self):
        logger.info('The DiracSplitter class is now depricated.')
        logger.info('As is the distinction of using it for Dirac jobs but SplitByFiles for non-Dirac jobs.')
        logger.info('One can now just use SplitByFiles for all jobs irrespective of backend.')
        super(DiracSplitter,self).__init__()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
