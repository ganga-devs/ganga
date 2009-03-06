#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Lib.File import FileBuffer, File
import Ganga.Utility.logging
import DiracShared
import DiracUtils
from GangaLHCb.Lib.Gaudi.RTHUtils import *
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from DiracScript import DiracScript
from GangaLHCb.Lib.Dirac.DiracUtils import mangleJobName

logger = Ganga.Utility.logging.getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiDiracRunTimeHandler(IRuntimeHandler):
    '''The runtime handler to run Gaudi jobs on the Dirac backend'''

    def __init__(self):
        pass

    def master_prepare(self,app,appconfig):
        '''Prepare the master configuration for the job.'''
        sandbox = get_master_input_sandbox(app.getJobObject(),app.extra) 
        return StandardJobConfig( '',sandbox,[],[],None)

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        '''Configure the specific parts of the subjob.'''

        s = '\nFileCatalog.Catalogs += '\
            '{ "xmlcatalog_file:pool_xml_catalog.xml" };\n'
        app.extra.input_buffers['data.opts'] += s
        
        sandbox = get_input_sandbox(app.extra)
        outputsandbox = app.extra.outputsandbox        
        runScript = self._create_dirac_script(app)
        
        c = StandardJobConfig(runScript,sandbox,[],outputsandbox,None)

        job = app.getJobObject()
        logFile = '%s_%s.log' % (app._name, app.version)
        
        diracScript = DiracScript()
        diracScript.platform(app.platform)
        diracScript.runApplicationScript(app.get_gaudi_appname(), app.version,
                                         DiracShared.getGenericRunScript(job),
                                         logFile)
        
        diracScript.setName(mangleJobName(job))
        
        if(app.extra.inputdata):
            diracScript.inputdata(app.extra.inputdata)
            if hasattr(app.extra.inputdata,'depth'):
                diracScript.ancestordepth(app.extra.inputdata.depth)

        diracScript.outputdata(app.extra.outputdata)

        c.script = diracScript
        c.logfile = logFile

        return c

    def _create_dirac_script(self, app):
        '''Create the script that will be executed.'''
        commandline = "'python ./gaudiPythonwrapper.py'"
        if is_gaudi_child(app):
            commandline = "'gaudirun.py options.pkl data.opts'"
        logger.debug( 'Command line: %s: ', commandline )
        wrapperscript = DiracUtils.gaudi_dirac_wrapper(commandline)

        logger.debug('Script to run on worker node\n'+wrapperscript)
        scriptName = "GaudiWrapper.py"
        runScript = FileBuffer(scriptName, wrapperscript, executable=1)

        return runScript

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
