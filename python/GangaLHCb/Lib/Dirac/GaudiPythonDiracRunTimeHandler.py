#!/usr/bin/env python

__author__ = 'Ulrik Egede'
__date__ = 'August 2008'
__revision__ = 0.1

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Lib.File import FileBuffer, File
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class GaudiPythonDiracRunTimeHandler(IRuntimeHandler):
    '''The runtime handler to run Gaudi jobs on the Dirac backend'''

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
        job = app.getJobObject()
        sandbox = []
        sandbox.append( FileBuffer('gaudiPythonwrapper.py',
                                   self.create_wrapperscript(app,extra)))

        dataopts = app.dataopts +\
                   '\nFileCatalog.Catalogs += { "xmlcatalog_file:pool_xml_catalog.xml" };\n'
        sandbox.append( FileBuffer('data.opts',dataopts))

        outsb = []
        for f in job.outputsandbox:
            outsb.append(f)

        logger.debug("Input sandbox: %s: ",str(sandbox))
        logger.debug("Output sandbox: %s: ",str(outsb))

        from DiracScript import DiracScript
        diracScript=DiracScript()
        
        runScript = self._DiracWrapper(app)

        c = StandardJobConfig( runScript,sandbox,[],outsb,None)
        
        diracScript.append( 'setApplication("' + app.project + '","' + app.version + '")')
        diracScript.platform(app.platform)
        diracScript.append( 'setName("Ganga_GaudiPython")')
        if job.inputdata:
            diracScript.inputdata(job.inputdata)

        outdata = []
        if job.outputdata:
            outdata += [ f.name for f in job.outputdata.files]
        diracScript.outputdata( outdata)

        c.script=diracScript
        c.logfile='%s_%s.log' % (app.project, app.version)

        return c

    def _DiracWrapper(self, app):
        '''Create the script that will be executed.'''

        commandline = "'python ./gaudiPythonwrapper.py'"
        logger.debug( 'Command line: %s: ', commandline )

        # Write a wrapper script
        wrapperscript= """#!/usr/bin/env python
'''Script to run Gaudi application'''
def setEnvironment(key, value, update=False):
    '''Sets an environment variable. If update=True, it preends it to
    the current value with os.pathsep as the seperator.'''
    from os import environ,pathsep
    if update and environ.has_key(key):
        value += (pathsep + environ[key])#prepend
    environ[key] = value

# Main
if __name__ == '__main__':

    from os import curdir, system, environ, pathsep, sep
    from os.path import join
    import sys    

    commandline = ###COMMANDLINE###    

    sys.stdout.flush()
    sys.stderr.flush()

    #exec the script
    print 'Executing ',commandline
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(system(commandline)/256)
  """
        wrapperscript = wrapperscript.replace('###COMMANDLINE###',commandline)

        logger.debug('Script to run on worker node\n'+wrapperscript)
        scriptName = "GaudiWrapper.py"
        runScript = FileBuffer( scriptName, wrapperscript, executable = 1 )

        return runScript

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
  
