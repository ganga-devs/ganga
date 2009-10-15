################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: SFrameARA.py,v 1.2 2008/11/24 16:12:50 mbarison Exp $
################################################################################
import os, socket, pwd, commands, re, string

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Base import GangaObject

from Ganga.Utility.Config import makeConfig, getConfig, ConfigError
from Ganga.Utility.logging import getLogger

from GangaSFrame.Lib.SFrame import SFrameApp
from GangaAtlas.Lib.Athena import Athena

class SFrameARA(IApplication):
    
    _schema = Schema(Version(2,0), {
        'exclude_from_user_area' : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='Pattern of files to exclude from user area'),
        'atlas_cmtconfig'        : SimpleItem(defvalue='',doc='ATLAS CMTCONFIG environment variable'),
        'atlas_environment'      : SimpleItem(defvalue=[], typelist=['str'], sequence=1, doc='Extra environment variable to be set'),
        'atlas_exetype'          : SimpleItem(defvalue='ATHENA',doc='Athena Executable type, e.g. ATHENA, PYARA, ROOT '),
        'atlas_production'       : SimpleItem(defvalue='',doc='ATLAS Production Software Release'),
        'atlas_project'          : SimpleItem(defvalue='',doc='ATLAS Project Name'),
        'atlas_release'          : SimpleItem(defvalue='',doc='ATLAS Software Release'),
        'atlas_dbrelease'        : SimpleItem(defvalue='',doc='ATLAS DBRelease DQ2 dataset and DQ2Release tar file'),
        'exclude_package'        : SimpleItem(defvalue = [], typelist=['str'], sequence=1,doc='Packages to exclude from user area requirements file'),
        'env'                    : SimpleItem(defvalue={},doc='Environment'),
        'group_area'             : FileItem(doc='A tar file of the group area'),
        'max_events'             : SimpleItem(defvalue='',doc='Maximum number of events'),
        'option_file'            : FileItem(defvalue = [], typelist=['str'], sequence=1, strict_sequence=0, doc="list of job options files" ),
        'options'                : SimpleItem(defvalue='',doc='Additional Athena options'),
        'sframe_archive'         : FileItem(doc='A tar file of the SFrame libraries'),
        'sframe_dir'             : FileItem(defvalue=File(os.environ['HOME']+'/SFrame'),doc="The directory containing the SFrame lib/ and bin/ directories"),
        'trf_parameter'          : SimpleItem(defvalue={},typelist=["dict","str"], doc='Parameters for transformations'),
        'exclude_list'    : SimpleItem(defvalue=[ "CVS", "obj", "*~", "*.root",
                                                  "*.ps", "*.so", "*.d", "*.rootmap", "*.pyc",
                                                  "*._Dict.*", "*.o", "python" ],
                                       doc='Patterns to be excluded from the compiled archive'),
        'user_area'              : FileItem(doc='A tar file of the user area'),
        'user_setupfile'         : FileItem(doc='User setup script for special setup'),
        'xml_options'            : FileItem(doc='A XML File specifying the SFrame options.'),
        'user_email' : SimpleItem(defvalue='', doc='email for job status notifications'),
        'recex_type'             : SimpleItem(defvalue = '',doc='Set to RDO, ESD or AOD to enable RecExCommon type jobs of appropriate type'),
        } )
    
    _category = 'applications'
    _name = 'SFrameARA'
    _exportmethods = ['prepare']
    _GUIPrefs = [
        { 'attribute' : 'exclude_from_user_area', 'widget' : 'FileOrString_List' },
        { 'attribute' : 'atlas_cmtconfig', 'widget' : 'String' },
        { 'attribute' : 'atlas_environment', 'widget' : 'String_List' },
        { 'attribute' : 'atlas_exetype',   'widget' : 'String_Choice', 'choices':['ATHENA', 'PYARA', 'ROOT' ]},
        { 'attribute' : 'atlas_production', 'widget' : 'String' },
        { 'attribute' : 'atlas_project', 'widget' : 'String' },
        { 'attribute' : 'atlas_release', 'widget' : 'String' },
        { 'attribute' : 'env', 'widget' : 'DictOfString' },
        { 'attribute' : 'group_area',     'widget' : 'FileOrString' },
        { 'attribute' : 'max_events',    'widget' : 'String' },
        { 'attribute' : 'option_file',   'widget' : 'FileOrString_List' },
        { 'attribute' : 'options',       'widget' : 'String_List' },
        { 'attribute' : 'sframe_archive', 'widget' : 'FileOrString' },
        { 'attribute' : 'sframe_dir', 'widget' : 'FileOrString' },
        { 'attribute' : 'exclude_package', 'widget' : 'FileOrString_List' },
        { 'attribute' : 'exclude_list', 'widget' : 'FileOrString_List' },
        { 'attribute' : 'user_area',     'widget' : 'FileOrString' },
        { 'attribute' : 'user_setupfile', 'widget' : 'FileOrString' },
        { 'attribute' : 'xml_options', 'widget' : 'File' },
        { 'attribute' : 'user_email', 'widget' : 'String' },
                  ]

    def __init__(self):
        super(SFrameARA, self).__init__()
              
        return

    def prepare(self, athena_compile=True, sframe_compile=True):

        self.SFrameSlave = SFrameApp()
        self.AthenaSlave = Athena()

        self.syncConfig(masterClean = False)

        self.AthenaSlave.prepare_old(athena_compile)  
        self.syncConfig(athenaClean = False)

        self.SFrameSlave.prepare(sframe_compile)
        self.syncConfig(sframeClean = False)

        return

    def configure(self,masterappconfig):
        logger.debug('SFrameARA configure called')
        return (None,None)

    def master_configure(self):

        logger.debug('SFrameARA master_configure called')

        if self.sframe_archive.name:
            if not self.sframe_archive.exists():
                raise ApplicationConfigurationError(None,'The tar file %s with the SFrame archive does not exist.' % self.sframe_archive.name)


        if self.user_area.name:
            if not self.user_area.exists():
                raise ApplicationConfigurationError(None,'The tar file %s with the user area does not exist.' % self.user_area.name)

        if self.group_area.name:
            if string.find(self.group_area.name,"http")<0 and not self.group_area.exists():
                raise ApplicationConfigurationError(None,'The tar file %s with the group area does not exist.' % self.group_area.name)
       
        if self.xml_options.name:
            if not self.xml_options.exists():
                raise ApplicationConfigurationError(None,'The XML option file %s does not exist.' % self.xml_options.name)


        job = self.getJobObject()

        if job.inputdata:
            if job.inputdata._name == 'DQ2Dataset':
                if job.inputdata.dataset and not job.inputdata.dataset_exists():
                    raise ApplicationConfigurationError(None,'DQ2 input dataset %s does not exist.' % job.inputdata.dataset)
                if job.inputdata.tagdataset and not job.inputdata.tagdataset_exists():
                    raise ApplicationConfigurationError(None,'DQ2 tag dataset %s does not exist.' % job.inputdata.tagdataset)

        return (0,None)


    def syncConfig(self, masterClean = True, athenaClean = True, sframeClean = True):

        self._setDirty(1)

        # Copy config between objects in this order:
        # Master to slaves
        # Athena to master
        # SFrame to master

        athenaDict = self.AthenaSlave.__dict__['_data']
        sframeDict = self.SFrameSlave.__dict__['_data']

        if not masterClean:
            # get list of schema attributes and pass them to slave classes
            for k, v in self._schema.allItems():
                logger.debug("Found property %s" % k)
                if True: #self._schema.getDefaultValue(k) != self._schema.getItem(k):
                    
                    if sframeDict.has_key(k):
                        logger.debug("Setting SFrameApp parameter '%s' to value '%s'" % (k,v))
                        exec("self.SFrameSlave.%s = self.%s" % (k,k))
                    if athenaDict.has_key(k):
                        logger.debug("Setting Athena parameter '%s' to value '%s'" % (k,v))
                        exec("self.AthenaSlave.%s = self.%s" % (k,k)) 
                       
            pass

    
        if not athenaClean:
            # get list of schema attributes and pass them to slave classes
            for k, v in athenaDict.iteritems():
                if self._schema.hasAttribute(k):
                    logger.debug("Setting SFrameARA parameter '%s' to value '%s'" % (k,v))
                    exec("self.%s = self.AthenaSlave.%s" % (k,k))
                    
            self.syncConfig(masterClean = False)
            pass

        if not sframeClean:
            # get list of schema attributes and pass them to slave classes
            for k, v in sframeDict.iteritems():
                if self._schema.hasAttribute(k):
                    logger.debug("Setting SFrameARA parameter '%s' to value '%s'" % (k,v))
                    exec("self.%s = self.SFrameSlave.%s" % (k,k))
                    
            self.syncConfig(masterClean = False)
            pass        

        return
    
config = makeConfig('SFrameARA','SFrameARA configuration parameters')
logger = getLogger('SFrameARA')

#$Log: SFrameARA.py,v $
#Revision 1.2  2008/11/24 16:12:50  mbarison
#*** empty log message ***
#
#Revision 1.1  2008/11/19 15:43:01  mbarison
#first version
#
