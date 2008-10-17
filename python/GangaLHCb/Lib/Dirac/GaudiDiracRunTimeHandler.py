#!/usr/bin/env python

__author__ = ' Andrew Maier, Greig A Cowan'
__date__ = 'June 2008'
__revision__ = 0.2

import os
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Lib.File import FileBuffer, File
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

import DiracShared

class GaudiDiracRunTimeHandler(IRuntimeHandler):
    '''The runtime handler to run Gaudi jobs on the Dirac backend'''

    def __init__(self):
        pass

    def master_prepare(self,app,appconfig):
        '''Prepare the master configuration for the job.'''
        inputsandbox=app._getParent().inputsandbox[:]
        
        inputsandbox.append( FileBuffer('options.pkl', app.extra.opts_pkl_str))

        for dll in app.extra._userdlls:
            inputsandbox.append( File( dll, subdir = 'lib'))
        for confDB in app.extra._merged_confDBs:
            inputsandbox.append( File( confDB, subdir = 'python'))
        for dir, files in app.extra._subdir_confDBs.iteritems():
            for f in files:
                inputsandbox.append( File( f, subdir = 'python' + os.sep + dir))   
                
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        c = StandardJobConfig( '',inputsandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        '''Configure the specific parts of the subjob.'''
        job = app.getJobObject()
        
        from DiracScript import DiracScript
        diracScript=DiracScript()
        
        inputsandbox = []
        if app.extra.dataopts:
            dataopts = app.extra.dataopts +\
                       '\nFileCatalog.Catalogs += { "xmlcatalog_file:pool_xml_catalog.xml" };\n'
            inputsandbox.append( FileBuffer( 'dataopts.opts', dataopts))
        
        outputsandbox = app._getParent().outputsandbox + app.extra._outputfiles

        logger.debug( 'Input sandbox: %s: ',str(inputsandbox))
        logger.debug( 'Output sandbox: %s: ',str(outputsandbox))

        runScript = self._DiracWrapper(app)

        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        c = StandardJobConfig( runScript,inputsandbox,[],outputsandbox,None)
        
        logFile = '%s_%s.log' % (app._name, app.version)
        
        diracScript.platform(app.platform)
        diracScript.runApplicationScript(app._name, app.version,\
                                         DiracShared.getGenericRunScript(),logFile)
        diracScript.setName( 'Ganga_%s_%s' % (app._name, app.version) )
        diracScript.inputdata( app.extra.inputdata)

        outdata = app.extra.outputdata
        if job.outputdata:
            outdata += [ f.name for f in job.outputdata.files]
        diracScript.outputdata( outdata)

        c.script=diracScript
        c.logfile=app._name+'_'+app.version+'.log'

        return c

    def _DiracWrapper(self, app):
        '''Create the script that will be executed.'''

#        commandline = '\'$GAUDIROOT/scripts/gaudirun.py options.pkl dataopts.py\''
        commandline = "'gaudirun.py options.pkl dataopts.opts'"
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

    from os import curdir, system, environ, pathsep, sep, getcwd
    from os.path import join
    import sys    

    commandline = ###COMMANDLINE###    

    sys.stdout.flush()
    sys.stderr.flush()
    setEnvironment( 'LD_LIBRARY_PATH', getcwd() + '/lib', True)
    setEnvironment( 'PYTHONPATH', getcwd() + '/python', True)
        
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
