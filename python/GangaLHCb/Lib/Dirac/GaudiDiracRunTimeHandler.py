#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

__author__ = ' Andrew Maier, Greig A Cowan'

import os
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Lib.File import FileBuffer, File
import Ganga.Utility.logging
import DiracShared
import DiracUtils
from GangaLHCb.Lib.Dirac.DiracUtils import mangleJobName

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

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
        for pyFile in app.extra._merged_pys:
            inputsandbox.append( File( pyFile, subdir = 'python'))
        for dir, files in app.extra._subdir_pys.iteritems():
            for f in files:
                inputsandbox.append(File(f, subdir = 'python' + os.sep + dir))
                
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
            dataopts = app.extra.dataopts +  '\nFileCatalog.Catalogs += ' \
                       '{ "xmlcatalog_file:pool_xml_catalog.xml" };\n'
            inputsandbox.append( FileBuffer( 'dataopts.opts', dataopts))
        
        outputsandbox = app.extra.outputsandbox

        logger.debug( 'Input sandbox: %s: ',str(inputsandbox))
        logger.debug( 'Output sandbox: %s: ',str(outputsandbox))

        runScript = self._DiracWrapper(app)

        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        c = StandardJobConfig( runScript,inputsandbox,[],outputsandbox,None)
        
        logFile = '%s_%s.log' % (app._name, app.version)
        
        diracScript.platform(app.platform)
        diracScript.runApplicationScript(app._name, app.version,
                                         DiracShared.getGenericRunScript(job),
                                         logFile)
        diracScript.setName(mangleJobName(job))
        diracScript.inputdata( app.extra.inputdata)
        if hasattr(app.extra.inputdata,'depth'):
            diracScript.ancestordepth(app.extra.inputdata.depth)
        diracScript.outputdata(app.extra.outputdata)

        c.script=diracScript
        c.logfile=app._name+'_'+app.version+'.log'

        return c

    def _DiracWrapper(self, app):
        '''Create the script that will be executed.'''

        commandline = "'gaudirun.py options.pkl dataopts.opts'"
        logger.debug( 'Command line: %s: ', commandline )
        wrapperscript = DiracUtils.gaudi_dirac_wrapper(commandline)

        logger.debug('Script to run on worker node\n'+wrapperscript)
        scriptName = "GaudiWrapper.py"
        runScript = FileBuffer(scriptName, wrapperscript, executable=1)

        return runScript

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
