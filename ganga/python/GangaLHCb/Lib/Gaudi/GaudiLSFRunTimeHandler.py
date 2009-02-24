#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Provide a run-time handler for Gaudi applications the LSF backend.'''

__author__ = ' Andrew Maier, Greig A Cowan'
__date__ = "$Date: 2009-02-19 11:07:03 $"
__revision__ = "$Revision: 1.8 $"

import os
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import FileBuffer, File
from Ganga.Utility.files import expandfilename
import Ganga.Utility.logging
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from GaudiUtils import create_lsf_runscript, gen_catalog

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiLSFRunTimeHandler(IRuntimeHandler):
  """This is the application runtime handler class for Gaudi applications 
  using the LSF backend."""
  
  def __init__(self):
    pass
  
  def prepare(self,app,extra,appmasterconfig,jobmasterconfig):
    
    sandbox = []
    if extra.dataopts:
      sandbox.append(FileBuffer("dataopts.opts",extra.dataopts))

    logger.debug("extra dataopts: " + str(extra.dataopts))
    logger.debug("Input sandbox: %s: ",str(sandbox))
    logger.debug("Output sandbox: %s: ",str(extra.outputsandbox))

    script=self.create_runscript(app,extra)

    return StandardJobConfig(FileBuffer('myscript',script,executable=1),
                             inputbox=sandbox,args=[],
                             outputbox=extra.outputsandbox)
      
  def master_prepare(self,app,extra):
    
    job = app.getJobObject()
    logger.debug("Entering the master_prepare of the GaudiLSF Runtimehandler") 
    logger.debug("extra dlls: %s", str(extra._userdlls))
    logger.debug("extra .py files: %s", str(extra._merged_pys))
    logger.debug("extra .py files: %s", str(extra._subdir_pys))
    
    sandbox =  [File(f,subdir='lib') for f in extra._userdlls]
    sandbox += [File(f,subdir='python') for f in extra._merged_pys]
    for dir, files in extra._subdir_pys.iteritems():
        sandbox += [File(f,subdir='python'+os.sep+dir) for f in files]
    sandbox += job.inputsandbox
    
    sandbox.append(FileBuffer('options.pkl', extra.opts_pkl_str))
    if extra.inputdata and extra.inputdata.hasLFNs():
      xml_catalog_str = gen_catalog(extra.inputdata, extra._LocalSite)
      sandbox.append(FileBuffer('myFiles.xml', xml_catalog_str))
    logger.debug("Input sandbox: %s: ",str(sandbox))
    
    return StandardJobConfig( '', inputbox=sandbox, args=[])

  def create_runscript(self,app,extra):
    
    logger.debug("Creating run script using GaudiLSFRunTimeHandler")
    appname = app.appname
    job = app.getJobObject()
    package = app.package
    user_release_area = expandfilename(app.user_release_area)
    xml_catalog = 'myFiles.xml'
    opts = 'options.pkl'    
    outputdata = extra.outputdata

    return create_lsf_runscript(app,appname,xml_catalog,package,opts,
                                user_release_area,outputdata,job,'Gaudi')


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
