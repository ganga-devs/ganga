#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import tempfile
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.Config
from Ganga.Utility.files import expandfilename
from Ganga.GPIDev.Lib.File import FileBuffer, File
import Ganga.Utility.logging
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *
import GangaLHCb.Lib.Applications.AppsBaseUtils
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import diracAPI_script_template
logger = Ganga.Utility.logging.getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def jobid_as_string(job):
    jstr=''
    if job.master: jstr=str(job.master.id)+os.sep+str(job.id)
    else: jstr=str(job.id)
    return jstr


def lhcbdiracAPI_script_template():

    DiracScript = diracAPI_script_template()
    DiracScript = DiracScript.replace('outputPath','OutputPath').replace('outputSE','OutputSE')

    DiracScript = DiracScript.replace('j.setName(\'###NAME###\')',
                  'j.setName(\'###NAME###\')\nj.setRootMacro(\'###ROOT_VERSION###\', \'###ROOT_MACRO###\', ###ROOT_ARGS###, \'###ROOT_LOG_FILE###\', systemConfig=\'###PLATFORM###\')' )

    DiracScript = DiracScript.replace('j.setName(\'###NAME###\')',
                  'j.setName(\'###NAME###\')\nj.setRootPythonScript(\'###ROOTPY_VERSION###\', \'###ROOTPY_SCRIPT###\', ###ROOTPY_ARGS###, \'###ROOTPY_LOG_FILE###\', systemConfig=\'###PLATFORM###\')' )

    DiracScript = DiracScript.replace('j.setName(\'###NAME###\')',
                  'j.setName(\'###NAME###\')\nj.setApplicationScript(\'###APP_NAME###\',\'###APP_VERSION###\',\'###APP_SCRIPT###\',logFile=\'###APP_LOG_FILE###\', systemConfig=\'###PLATFORM###\')' )

    DiracScript = DiracScript.replace('\'###EXE_LOG_FILE###\'','\'###EXE_LOG_FILE###\', systemConfig=\'###PLATFORM###\'')
    DiracScript = DiracScript.replace('j.setPlatform( \'ANY\' )', 'j.setDIRACPlatform()' )
    DiracScript = DiracScript.replace('###OUTPUT_SE###','###OUTPUT_SE###,replicate=\'###REPLICATE###\'' )

    #print DiracScript
    return DiracScript


## def get_master_input_sandbox(job,extra):
##     sandbox = job.inputsandbox[:]
##     sandbox += extra.master_input_files[:]
##     buffers = extra.master_input_buffers
##     sandbox += [FileBuffer(n,s) for (n,s) in buffers.items()]
##     logger.debug("Master input sandbox: %s",str(sandbox))
##     return sandbox

## def get_input_sandbox(extra):
##      sandbox = []
##      sandbox += extra.input_files[:]
##      sandbox += [FileBuffer(n,s) for (n,s) in extra.input_buffers.items()]
##      logger.debug("Input sandbox: %s",str(sandbox))
##      return sandbox

def is_gaudi_child(app):
    if app.__class__.__name__ == 'Gaudi' \
           or type(app).__bases__[0].__name__ == 'Gaudi':
        return True
    
    if type(app).__bases__[0].__name__ == 'TaskApplication':
        if not app.__class__.__name__ == 'GaudiPythonTask' \
               and not app.__class__.__name__ == 'BenderTask' :
            return True
    
    return False

class filenameFilter:
    def __init__(self, filename):
        self.filename = filename

    def __call__(self, file):
        return file.name == self.filename


def getXMLSummaryScript(indent=''):
  '''Returns the necessary script to parse and make sense of the XMLSummary data'''
  import inspect
  from GangaLHCb.Lib.Applications.AppsBaseUtils import activeSummaryItems
  script  = "###INDENT#### Parsed XMLSummary data extraction methods\n"
  
  for summaryItem in activeSummaryItems().values():
    script += ''.join(['###INDENT###'+line for line in inspect.getsourcelines(summaryItem)[0]])
  script += ''.join(['###INDENT###'+line for line in inspect.getsourcelines(activeSummaryItems)[0]])
##     script += inspect.getsource(summaryItem)
##   script += inspect.getsource(activeSummaryItems)

  script += """
###INDENT#### XMLSummary parsing
###INDENT###import os, sys
###INDENT###if 'XMLSUMMARYBASEROOT' not in os.environ:
###INDENT###    sys.stderr.write(\"\'XMLSUMMARYBASEROOT\' env var not defined so summary.xml not parsed\")
###INDENT###else:
###INDENT###    schemapath  = os.path.join(os.environ['XMLSUMMARYBASEROOT'],'xml/XMLSummary.xsd')
###INDENT###    summarypath = os.path.join(os.environ['XMLSUMMARYBASEROOT'],'python/XMLSummaryBase')
###INDENT###    sys.path.append(summarypath)
###INDENT###    import summary
###INDENT###
###INDENT###    outputxml = os.path.join(os.getcwd(), 'summary.xml')
###INDENT###    if not os.path.exists(outputxml):
###INDENT###        sys.stderr.write(\"XMLSummary not passed as \'summary.xml\' not present in working dir\")
###INDENT###    else:
###INDENT###        try:
###INDENT###            XMLSummarydata = summary.Summary(schemapath,construct_default=False)
###INDENT###            XMLSummarydata.parse(outputxml)
###INDENT###        except:
###INDENT###            sys.stderr.write(\"Failure when parsing XMLSummary file \'summary.xml\'\")
###INDENT###
###INDENT###        # write to file
###INDENT###        #with open('__parsedxmlsummary__','w') as parsedXML: #removed for python 2.4 compatibility
###INDENT###        try:  
###INDENT###            fn = open('__parsedxmlsummary__','w')
###INDENT###            for name, method in activeSummaryItems().iteritems():
###INDENT###                try:
###INDENT###                    fn.write( '%s = %s\\n' % ( name, str(method(XMLSummarydata)) ) )
###INDENT###                except Exception, e:
###INDENT###                    #import traceback
###INDENT###                    fn.write( '%s = None\\n' % name )
###INDENT###                    #sys.stderr.write('XMLSummary error: Failed to run the method \"%s\"\\n%s\\n' % (name,traceback.format_exc()))
###INDENT###                    sys.stderr.write('XMLSummary warning: Method \"%s\" not available for this job\\n' % name)
###INDENT###        except:
###INDENT###            sys.stderr.write('XMLSummary error: Failed to create __parsedxmlsummary__ file')
###INDENT###        finally:
###INDENT###            fn.close()  
"""
  return script.replace('###INDENT###',indent)

def create_runscript():

  script = """#!/usr/bin/env python

import os,sys
opts = '###OPTS###'
project_opts = '###PROJECT_OPTS###'
app = '###APP_NAME###'
app_upper = app.upper()
version = '###APP_VERSION###'
package = '###APP_PACKAGE###'
platform = '###PLATFORM###'


# check that options file exists
if not os.path.exists(opts):
    opts = 'notavailable'
    os.environ['JOBOPTPATH'] = opts
else:
    os.environ['JOBOPTPATH'] = os.path.join(os.environ[app + '_release_area'],
                                            app_upper,
                                            app_upper,
                                            version,
                                            package,
                                            app,
                                            version,
                                            'options',
                                            'job.opts')
    print 'Using the master optionsfile:', opts
    sys.stdout.flush()
    

# check that SetupProject.sh script exists, then execute it
os.environ['User_release_area'] = ''  
#os.environ['CMTCONFIG'] = platform  
f=os.popen('which SetupProject.sh')
setup_script=f.read()[:-1]
f.close()
if os.path.exists(setup_script):
    os.system('''/usr/bin/env bash -c '. `which LbLogin.sh` -c %s && source %s %s %s %s && printenv > \
env.tmp' ''' % (platform, setup_script,project_opts,app,version))
    for line in open('env.tmp').readlines():
        varval = line.strip().split('=')
        if len(varval) < 2:
            pass
        else:
            content = ''.join(varval[1:])
            # Lets drop bash functions
            if not str(content).startswith('() {'):
                os.environ[varval[0]] = content
    os.system('rm -f env.tmp')
else:
    print 'Could not find %s. Your job will probably fail.' % setup_script
    sys.stdout.flush()
        
# add lib subdir in case user supplied shared libs where copied to pwd/lib
os.environ['LD_LIBRARY_PATH'] = '.:%s/lib:%s\' %(os.getcwd(),
                                                 os.environ['LD_LIBRARY_PATH'])
                                                 
#run
sys.stdout.flush()
os.environ['PYTHONPATH'] = '%s/InstallArea/python:%s' % \\
                            (os.getcwd(), os.environ['PYTHONPATH'])
os.environ['PYTHONPATH'] = '%s/InstallArea/%s/python:%s' % \\
                            (os.getcwd(), platform,os.environ['PYTHONPATH'])

cmdline = '''###CMDLINE###'''

# run command
os.system(cmdline)

###XMLSUMMARYPARSING###
"""
  return script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
