#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""Provide a Application runtime handler for GaudiPython applications for
the backends sharing the local filesystem."""

__author__ = 'Ulrik Egede'
__date__ = "$Date: 2009-01-28 13:18:19 $"
__revision__ = "$Revision: 1.6 $"

from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
import os,os.path
from Ganga.GPIDev.Lib.File import FileBuffer
import Ganga.Utility.Config 
from Ganga.Utility.util import unique
import Ganga.Utility.logging
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from GaudiUtils import create_lsf_runscript,collect_lhcb_filelist

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiPythonLSFRunTimeHandler(IRuntimeHandler):
  """This is the application runtime handler class for GaudiPython
  applications which use a share local file system"""
  
  def __init__(self):
    pass
  
  def master_prepare(self,app,extra):
    
    logger.debug("Entering master_prepare of GaudiPythonLSFRunTimeHandler")
    job = app.getJobObject()

    sandbox = [f for f in job.inputsandbox]
    sandbox += [script for script in job.application.script]
    if(app.extra.xml_catalog_str):
      sandbox.append(FileBuffer('myFiles.xml', app.extra.xml_catalog_str))
    logger.debug("Master input sandbox: %s: ",str(sandbox))

    return StandardJobConfig( '', inputbox=sandbox, args=[])


  def prepare(self,app,extra,appmasterconfig,jobmasterconfig):
    
    job= app.getJobObject()
    
    sandbox = [FileBuffer('gaudiPythonwrapper.py',
                          self.create_wrapperscript(app,extra))]
    sandbox.append(FileBuffer('dataopts.opts',app.dataopts))
    
    outsb = collect_lhcb_filelist(job.outputsandbox)
    
    logger.debug("Input sandbox: %s: ",str(sandbox))
    logger.debug("Output sandbox: %s: ",str(outsb))
    script = self.create_runscript(app,extra)

    return StandardJobConfig(FileBuffer('myscript',script,executable=1),
                             inputbox=sandbox,args=[],outputbox=unique(outsb))
      
  def create_runscript(self,app,extra):

    logger.debug("Creating run script using GaudiPythonLSFRunTimeHandler")
    job = app.getJobObject()
    config = Ganga.Utility.Config.getConfig('LHCb') 
    appname = app.project
    outputdata = collect_lhcb_filelist(job.outputdata)
    job = app.getJobObject()

    package = None
    user_release_area = None
    opts = None

    return create_lsf_runscript(app,appname,'myFiles.xml',package,opts,
                                user_release_area,outputdata,job,'GaudiPython')

    
  def create_wrapperscript(self,app,extra):
    from os.path import split,join
    name = join('.',app.script[0].subdir,split(app.script[0].name)[-1])
    script =  "from Gaudi.Configuration import *\n"
    script += "importOptions('dataopts.opts')\n"
    script += "execfile(\'%s\')\n" % name    
    return script
  
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
