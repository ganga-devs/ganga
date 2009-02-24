################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: MergerTester.py,v 1.1 2008-07-17 16:41:12 moscicki Exp $
################################################################################

from Ganga.GPIDev.Adapters.IMerger import MergerError
from Ganga.Lib.Mergers import Merger
from Ganga.GPIDev.Schema import SimpleItem

class _TestMergeTool(Merger.IMergeTool):
    _category = 'merge_tools'
    _hidden = 1
    _name = '_TestMergeTool'
    _schema = Merger.IMergeTool._schema.inherit_copy()
    _schema.datadict['alwaysfail'] = SimpleItem(defvalue = True, doc='Flag to set if the merge should always fail')
    _schema.datadict['wait'] = SimpleItem(defvalue = -1, doc='Time in seconds that the merge should sleep for.')

    def mergefiles(self, file_list, output_file):
        print 'merging'
        
        if self.wait > 0:
            print 'sleeping for %d seconds' % self.wait
            import time
            time.sleep(self.wait)
        
        if self.alwaysfail:
            raise MergerError('This merge will always fail as this is a test')

class MergerTester(Merger.AbstractMerger):
    _category = 'mergers'
    _exportmethods = ['merge']
    _name = 'MergerTester'
    _schema = Merger.AbstractMerger._schema.inherit_copy()
    _schema.datadict['alwaysfail'] = SimpleItem(defvalue = True, doc='Flag to set if the merge should always fail')
    _schema.datadict['wait'] = SimpleItem(defvalue = -1, doc='Time in seconds that the merge should sleep for.')

    def __init__(self):
        super(MergerTester,self).__init__(_TestMergeTool())
        self.merge_tool.alwaysfail = self.alwaysfail
        self.merge_tool.wait = self.wait

    def merge(self, jobs, outputdir = None, ignorefailed = None, overwrite = None):
        #needed as exportmethods doesn't seem to cope with inheritance
        return super(MergerTester,self).merge(jobs, outputdir, ignorefailed, overwrite)
    
