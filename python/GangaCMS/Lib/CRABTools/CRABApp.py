#
# CRAB Application
# 
# 08/06/10 @ ubeda
#

import subprocess

#from Ganga.Core import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import *
from Ganga.Utility.logging import getLogger

from GangaCMS.Lib.Utils import ConfigSection, ConfigFile, ConfigFileParser
from GangaCMS.Lib.ConfParams import *
from GangaCMS.Lib.CRABTools.CRABServer import *

logger = getLogger()

class CRABApp(IApplication):

    comments=[]
    comments.append('crab.cfg user file used to merge with parameters of CRABDataset (if any)')
    comments.append('If force is 1, crab.cfg parameters will be overwritten by job.inputdata parameters if there is overlapping.')
    comments.append('0 if you dont want to see crab -create logs.')
    comments.append('crab.cfg file created by GangaCMS.')
    comments.append('Workdir.')

    schemadic = {}
    schemadic['cfg_file']       = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[0])       
    schemadic['force']          = SimpleItem(defvalue=0,    typelist=['int']             , doc=comments[1])
    schemadic['verbose']        = SimpleItem(defvalue=1,    typelist=['int']             , doc=comments[2]) 
    schemadic['cfgfile']        = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[3], copiable=0, protected=1) 
    schemadic['workdir']        = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc=comments[4], copiable=0, protected=1) 

    _schema = Schema(Version(1,0), schemadic)
    _category = 'applications'
    _name = 'CRABApp'
  
    def __init__(self):
        super(CRABApp,self).__init__()

    def writeCRABFile(self,cfgfile):
    
        job = self.getJobObject()

        #Get cfg_file parsed or default template if not possible
        cfp = ConfigFileParser().getSchema()

        if self.cfg_file:
            cfp = ConfigFileParser(self.cfg_file).parse()
            logger.info('Custom crab file used.')

        #Open file for writting the new crab.cfg
        cfile = ConfigFile(cfgfile)

        # Merge crab.cfg and CRABDataset parameters
        # and write crab.cfg
        for section in cfp.keys():
            for key,item in cfp[section].schemadic.items():               
                attr = getattr(job.inputdata,key)
                value = None
                if cfp[section].schemadic[key].__class__.__name__ != 'SimpleItem':
                    value = cfp[section].schemadic[key]
                if attr != None:
                    if item.__class__.__name__ == 'SimpleItem' or self.force:
                        value = attr
                        if self.force and item.__class__.__name__ != 'SimpleItem':
                            logger.info('Forcing "%s:%s" in "%s" section.'%(key,attr,section))
                        else:
                            logger.info('Adding "%s:%s" in "%s" section.'%(key,attr,section))
                if value:
                    cfile.append(section,'%s=%s'%(key,value))
                    setattr(job.inputdata,key,value)

        #Ensures ui_working_dir is stablished.
        if job.inputdata.schemadic['ui_working_dir'].__class__.__name__ == 'SimpleItem':
            logger.info('Adding "%s:%s" in "%s" section.'%('ui_working_dir',job.outputdir,'USER'))
            cfile.append('USER','ui_working_dir=%s'%(job.outputdir))
            setattr(job.inputdata,'ui_working_dir',job.outputdir)

        cfile.write()

    def master_configure(self):
        
        #Get job containing this CRABApp
        job = self.getJobObject()
        
        #File where crab.cfg is going to be written
        cfgfile = '%scrab.cfg'%(job.inputdir)

        #Merge,update and write crab.cfg file
        job.application.writeCRABFile(cfgfile)

        logger.info('File crab.cfg can be found here: %s'%(cfgfile))
        logger.info('Workdir can be foud here: %s'%(job.inputdata.ui_working_dir))
        job.application.cfgfile = cfgfile
        job.application.workdir = job.inputdata.ui_working_dir
        
        server = CRABServer()
        server.create(job)
        return (1,None)

    def configure(self,masterappconfig):
        return (None,None)
