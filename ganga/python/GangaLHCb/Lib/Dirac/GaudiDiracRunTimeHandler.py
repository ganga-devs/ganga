from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Lib.File import FileBuffer,File
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class GaudiDiracRunTimeHandler(IRuntimeHandler):
    """The runtime handler to run Gaudi jobs on the Dirac backend"""

    def __init__(self):
        pass

    def master_prepare(self,app,appconfig):

        inputsandbox=app._getParent().inputsandbox[:]

        options = app.extra.flatopts +\
                  app._determine_catalog_type()+\
                  ' += { "xmlcatalog_file:pool_xml_catalog.xml" };\n'

        inputsandbox.append(FileBuffer('expandedopts.opts',options))

        for dll in app.extra._userdlls:
            inputsandbox.append(File(name=dll,subdir='lib'))

        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        c = StandardJobConfig('',inputsandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        job = app.getJobObject()
        
        from DiracScript import DiracScript
        diracScript=DiracScript()

        options = '#include "expandedopts.opts"\n' + app.extra.dataopts

        inputsandbox= [FileBuffer(self._optsfilename(app),options)]

        outputsandbox = app._getParent().outputsandbox + app.extra.outputfiles

        runScript = self._DiracWrapper(app)

        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        c = StandardJobConfig(runScript,inputsandbox,[],outputsandbox,None)
        diracScript.append('setApplication("'+app._name+'","'+app.version+'")')
        diracScript.append('setName("Ganga_'+ app._name+'_'+app.version+'")')


        diracScript.inputdata(app.extra.inputdata)

        outdata = app.extra.outputdata
        if job.outputdata:
            outdata += [f.name for f in job.outputdata.files]
        diracScript.outputdata(outdata)

        c.script=diracScript

        c.logfile=app._name+'_'+app.version+'.log'

        return c

    def _optsfilename(self,app):
        try:
            fullname = app.optsfile[0].name #FIXME opfile is now a sequence
        except:
            fullname='job.opts'

        from os.path import basename
        name = basename(fullname)
        if name=='': name='job.opts'
        return name


    def _DiracWrapper(self,app):
        from os.path import join,split
        from os import environ,pathsep

        commandline = ''
        arglist = []

        commandline = '\''+app._name+'.exe ' + self._optsfilename(app) + '\''
        logger.debug( "Command line: %s: ", commandline )

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
    # usepython = ###USEPYTHON###

    sys.stdout.flush()
    sys.stderr.flush()

    # if usepython:
    # 
    #     pythonCmd = 'python'
    #     commandline = commandline % {'PYTHONCMD':pythonCmd}

    #exec the script
    print 'Executing ',commandline
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(system(commandline)/256)
  """

        wrapperscript = wrapperscript.replace('###COMMANDLINE###',commandline)
        # wrapperscript = wrapperscript.replace('###USEPYTHON###',str(app.usepython))

        logger.debug('Script to run on worker node\n'+wrapperscript)
        scriptName = "GaudiWrapper.py"
        runScript = FileBuffer( scriptName, wrapperscript, executable = 1 )

        return runScript
