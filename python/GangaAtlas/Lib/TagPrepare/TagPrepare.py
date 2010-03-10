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

from pandatools import AthenaUtils


class TagPrepare(IApplication):
    """Run a preparation job for TAG analysis"""

    _schema = Schema(Version(2,2), {
        'atlas_release'          : SimpleItem(defvalue='',doc='ATLAS Software Release'),
        'tag_info'               : SimpleItem(defvalue={},doc='Returned TAG info'),
        'stream_ref'             : SimpleItem(defvalue='AOD',doc='The stream reference to use (AOD, ESD).'),
        'max_num_refs'           : SimpleItem(defvalue=1,doc='The maximum number of references retrieve per TAG file (defaults to a 1->1 mapping).'),
        'atlas_cmtconfig'        : SimpleItem(defvalue='',doc='ATLAS CMTCONFIG environment variable')
              })
                     
    _category = 'applications'
    _name = 'TagPrepare'
    _exportmethods = ['taginfo']

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

from Ganga.GPIDev.Adapters.IMerger import IMerger
from commands import getstatusoutput    
import threading
from GangaAtlas.Lib.ATLASDataset import Download
from GangaAtlas.Lib.ATLASDataset import filecheck
