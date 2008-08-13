#!/usr/bin/env python
"""
Provide a Application runtime handler for GaudiPython applications for
the backends sharing the local filesystem.
"""

__author__ = 'Ulrik Egede'
__date__ = 'August 2008'
__revision__ = 0.1

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

import sys,os,os.path
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.GPIDev.Lib.File import File
import Ganga.Utility.Config 
from Ganga.Utility.util import unique

import Ganga.Utility.logging

from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

logger = Ganga.Utility.logging.getLogger()
class GaudiPythonLSFRunTimeHandler(IRuntimeHandler):
  """This is the application runtime handler class for GaudiPython
  applications which use a share local file system"""
  
  def __init__(self):
    pass
  
  def master_prepare(self,app,extra):
    
    job = app.getJobObject()
    logger.debug("Entering the master_prepare of the GaudiPythonLSF Runtimehandler") 
    sandbox=[]
    sandbox += job.inputsandbox
    for script in job.application.script:
      sandbox.append(script)

    logger.debug("Master input sandbox: %s: ",str(sandbox))

    return StandardJobConfig( '', inputbox = sandbox, args=[])


  def prepare(self,app,extra,appmasterconfig,jobmasterconfig):
    
    job= app.getJobObject()

    sandbox = []
    sandbox.append( FileBuffer('gaudiPythonwrapper.py',
                               self.create_wrapperscript(app,extra)))
    sandbox.append( FileBuffer('data.opts',app.dataopts))

    outsb = []
    for f in job.outputsandbox:
        outsb.append(f)
    
    logger.debug("Input sandbox: %s: ",str(sandbox))
    logger.debug("Output sandbox: %s: ",str(outsb))
    script=self.create_runscript(app,extra)

    return StandardJobConfig(FileBuffer('myscript',script,executable=1),
                             inputbox=sandbox,args=[],outputbox=unique(outsb))
      
  def jobid_as_string(self,app):
      import os.path
      job=app.getJobObject()
      jstr=''
      # test is this is a subjobs or not
      if job.master: # it's a subjob
          jstr=str(job.master.id)+os.sep+str(job.id)
      else:
          jstr=str(job.id)
      return jstr

  def create_runscript(self,app,extra):
    
    job = app.getJobObject()
    config = Ganga.Utility.Config.getConfig('LHCb') 
    version = app.version
    theApp = app.project
    lhcb_release_area = app.lhcb_release_area
    platform = app.platform
    appUpper = theApp.upper()
    site = config['LocalSite']
    protocol = config['SEProtocol']
    jstr = self.jobid_as_string(app)
    copy_cmd = config['cp_cmd']
    mkdir_cmd = config['mkdir_cmd']
    joboutdir = config['DataOutput']
    
    if job.outputdata:
      outputdatastr = ' '.join(job.outputdata)
    else:
       outputdatastr = ''
    
    script="""#!/usr/bin/env bash
export LHCb_release_area=###CMTRELEASEAREA###
export DATAOUTPUT='###DATAOUTPUT###'
DATAOPTS='data.opts'
JOBOUTPUTDIR=###JOBOUTDIR###/###JOBID###/outputdata
CP=###COPY###
MKDIR=###MKDIR###

if [ -f ${LHCBHOME}/scripts/SetupProject.sh ]; then
  . ${LHCBHOME}/scripts/SetupProject.sh  ###THEAPP### ###VERSION###
else
  echo Could not find the SetupProject.sh script. Your job will probably fail
fi
# create an xml slice
if [ -f ${DATAOPTS} ]; then
    genCatalog -o ${DATAOPTS} -p myFiles.xml -s ###SITE### -P ###SEPROTOCOL### #ugly and hardcoded , FIXME
    echo >> ${DATAOPTS}
    echo 'FileCatalog.Catalogs += { "xmlcatalog_file:myFiles.xml" };' >> ${DATAOPTS}
fi    

python ./gaudiPythonwrapper.py

$MKDIR -p $JOBOUTPUTDIR
for f in $DATAOUTPUT; do 
    $CP "$f" "${JOBOUTPUTDIR}/$f"
    echo "Copying $f to $JOBOUTPUTDIR"
    if [ $? -ne 0 ]; then
       echo "WARNING:  Could not copy file $f to $JOBOUTPUTDIR"
       echo "WARNING:  File $f will be lost"
    fi
    rm -f "$f"
done
"""
    script=script.replace('###DATAOUTPUT###', outputdatastr)
    script=script.replace('###THEAPP###',theApp) 
    script=script.replace('###CMTRELEASEAREA###',lhcb_release_area)
    script=script.replace('###VERSION###',version)
    script=script.replace('###PLATFORM###',platform)
    script=script.replace('###JOBID###',jstr)
    script=script.replace('###JOBOUTDIR###',joboutdir)
    script=script.replace('###SITE###',site)
    script=script.replace('###SEPROTOCOL###',protocol)
    script=script.replace('###COPY###',copy_cmd)
    script=script.replace('###MKDIR###',mkdir_cmd)
    
    return script

  def create_wrapperscript(self,app,extra):
    script = """
from Gaudi.Configuration import *
importOptions('data.opts')
execfile('###SCRIPTNAME###')
"""
    from os.path import split,join
    name = join('.',app.script[0].subdir,split(app.script[0].name)[-1])
    script=script.replace('###SCRIPTNAME###',name)
    
    return script
  
