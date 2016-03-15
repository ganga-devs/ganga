##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: MergerTester.py,v 1.1 2008-07-17 16:41:12 moscicki Exp $
##########################################################################

from Ganga.GPIDev.Adapters.IMerger import IMerger
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
from Ganga.GPIDev.Schema import SimpleItem

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)


class MergerTester(IMerger):
    _category = 'postprocessor'
    _exportmethods = ['merge']
    _name = 'MergerTester'
    _schema = IMerger._schema.inherit_copy()
    _schema.datadict['alwaysfail'] = SimpleItem(
        defvalue=True, doc='Flag to set if the merge should always fail')
    _schema.datadict['wait'] = SimpleItem(
        defvalue=-1, doc='Time in seconds that the merge should sleep for.')

    def mergefiles(self, file_list, output_file):
        logger.info('merging')

        if self.wait > 0:
            logger.info('sleeping for %d seconds' % self.wait)
            import time
            time.sleep(self.wait)

        if self.alwaysfail:
            raise PostProcessException(
                'This merge will always fail as this is a test')
