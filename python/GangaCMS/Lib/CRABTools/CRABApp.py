#
# CRAB Application
# 
# 08/06/10 @ ubeda
#
import subprocess

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import *
from Ganga.Utility.logging import getLogger

from GangaCMS.Lib.ConfParams import *
from GangaCMS.Lib.CRABTools.CRABServer import *

import Ganga.Utility.Config

logger = getLogger()

class CRABApp(IApplication):

  comments=[]
  comments.append('crab.cfg file either created by GangaCMS or added by user.')
  comments.append('Workdir.')

  schemadic = {}
  schemadic['cfgfile']        = SimpleItem(defvalue=None,    typelist=['type(None)','str'], doc=comments[0], copiable=0) 
  schemadic['workdir']        = SimpleItem(defvalue=None,    typelist=['type(None)','str'], doc=comments[1], copiable=0) 

  # is_prepared is needed for GangaRobot on Ganga 5.7.0 and later.
  schemadic['is_prepared']    = SharedItem(defvalue=None,
                                           strict_sequence=0,
                                           visitable=1,
                                           copyable=1,
                                           typelist=['type(None)','bool','str'],
                                           protected=0,
                                           doc='Location of shared resources. Presence of this attribute implies the application has been prepared.')

  _schema = Schema(Version(1,0), schemadic)
  _category = 'applications'
  _name = 'CRABApp' 

  def __init__(self):
#    self.server = CRABServer()
    super(CRABApp,self).__init__()

  def writeCRABFile(self,job,cfg_file):

    file = open(cfg_file,'w')     

    job.inputdata.ui_working_dir = job.outputdir
                       
    for params in [CMSSW(),CRAB(),GRID(),USER()]:

      section = params.__class__.__name__

      config = Ganga.Utility.Config.getConfig('%s_CFG'%(section))
      file.write('['+section+']\n\n')

      for k in params.schemadic.keys():

        # We get the parametef from the config. If it is not NULL,
        # we use that, otherwise, we try to take it from inputdata.
        attr = config[k]
        if attr == None:
          attr = getattr(job.inputdata,k)

        if attr != None:  
          file.write('%s=%s\n'%(k,attr))                  
      file.write('\n')

    file.close()

  def master_configure(self):

    #Get job containing this CRABApp 
    job = self.getJobObject()

    #File where crab.cfg is going to be written
    cfg_file = '%scrab.cfg'%(job.inputdir)
    job.application.writeCRABFile(job,cfg_file)
    job.application.cfgfile = cfg_file

    job.application.workdir = job.inputdata.ui_working_dir

    server = CRABServer()
    server.create(job)
#    self.server.create(job)

    return (1,None)

  def configure(self,masterappconfig):
    return (None,None)
