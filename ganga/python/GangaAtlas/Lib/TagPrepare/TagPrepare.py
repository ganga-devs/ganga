###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TagPrepare.py,v 1.1 2009-21-09 13:40:03 mslater Exp $
###############################################################################
# Tag Prepare Application

import os, re, commands, string

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaAtlas.Lib.ATLASDataset import filecheck

from Ganga.Lib.Mergers.Merger import *

__directory__ = os.path.dirname(__file__)
logger = getLogger()

class TagPrepare(IApplication):
    """Run a preparation job for TAG analysis"""

    _schema = Schema(Version(2,2), {
        'atlas_release'          : SimpleItem(defvalue='',doc='ATLAS Software Release'),
        'tag_info'               : SimpleItem(defvalue={},doc='Returned TAG info'),
        'stream_ref'             : SimpleItem(defvalue='AOD',doc='The stream reference to use (AOD, ESD).'),
        'max_num_refs'           : SimpleItem(defvalue=3,doc='The maximum number of references retrieve per TAG file (defaults to 3).'),
        'atlas_cmtconfig'        : SimpleItem(defvalue='',doc='ATLAS CMTCONFIG environment variable'),
        'lcg_prepare'            : SimpleItem(defvalue=False,doc='Prepare an LCG TAG submission rather than Panda')
              })
                     
    _category = 'applications'
    _name = 'TagPrepare'
    _exportmethods = ['prepare_user_area']

    def postprocess(self):
        """Load in and sort out the tag info"""
        from Ganga.GPIDev.Lib.Job import Job
        job = self.getJobObject()

        if job.subjobs:
            # collect the subjob info
            self.tag_info = {}
            for j in job.subjobs:
                self.tag_info.update( j.application.tag_info )

            # loop over them and check for _subxxxx style datasets
            ref_datasets = []
            for tf in self.tag_info:
                if not self.tag_info[tf]['refs'][0][1] in ref_datasets:
                    ref_datasets.append( self.tag_info[tf]['refs'][0][1] )

            for tf in self.tag_info:
                dataset = self.tag_info[tf]['refs'][0][1]
                for ref in ref_datasets:
                    if dataset.find( (ref + '_sub') ) != -1:
                        self.tag_info[tf]['refs'][0][1] = ref
        else:
            import pickle
            self.tag_info = pickle.load( open(os.path.join( job.outputdir, "taginfo.pkl")) )
            if os.path.exists(os.path.join( job.outputdir, 'subcoll.tar.gz')):
                os.system('cd %s && tar -zxf %s/subcoll.tar.gz' % (job.outputdir, job.outputdir) )
            
    def configure(self,masterappconfig):
        logger.debug('TagPrepare configure called')
        return (None,None)

    def master_configure(self):

        logger.debug('TagPrepare master_configure called')
        job = self.getJobObject()

        if job.inputdata:
            if job.inputdata._name == 'DQ2Dataset':
                if job.inputdata.dataset and not job.inputdata.dataset_exists():
                    raise ApplicationConfigurationError(None,'DQ2 input dataset %s does not exist.' % job.inputdata.dataset)
            elif job.inputdata._name == 'ATLASLocalDataset':
                if len(job.inputdata.names) == 0:
                    raise ApplicationConfigurationError(None,'No inputdata specified in ATLASLocalDataset')

        if self.atlas_release == '':
            raise ApplicationConfigurationError(None,'No Atlas release specefied.')
        
        return (0,None)

    def prepare_user_area(self):
        "Copy appropriate files to a user area and return the required list"
        import shutil
        
        # copy the overall TAG info
        job = self.getJobObject()
        files_to_copy = ['%s/subcoll.tar.gz' % job.outputdir]

        # Now the reference files
        ref_files = []
        for tag in self.tag_info:
            toks = tag.split('.')
            stripname = '.'.join( toks[: len(toks) - 3] )

            if not stripname in ref_files:
                ref_files.append( stripname )

        for f in ref_files:
            files_to_copy.append( '%s/%s.ref.root' % (job.outputdir, f) )

        # and now the worker files
        for f in ['uncompress.py', 'template.root']:
            if f in os.listdir('.'):
                size = os.path.getsize('%s/%s' % (__directory__, f))
                if size != os.path.getsize(f):
                    raise ApplicationConfigurationError(None, "File '%s' already present in current dir. Please rename or remove before continuing." % f)
            
            files_to_copy.append('%s/%s' % (__directory__, f))

        filelist = []
        logger.warning('Copying the files required for TAG running to the current directory...')
        for f in files_to_copy:
            shutil.copyfile( f, '%s/%s' % (os.getcwd(), os.path.basename(f)))
            filelist.append( os.path.basename(f) )

        return filelist
    
from Ganga.GPIDev.Adapters.IMerger import IMerger
from commands import getstatusoutput    
import threading
from GangaAtlas.Lib.ATLASDataset import Download
from GangaAtlas.Lib.ATLASDataset import filecheck
