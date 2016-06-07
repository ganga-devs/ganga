#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import tempfile
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.Config
from Ganga.Utility.files import expandfilename
from Ganga.GPIDev.Lib.File import FileBuffer, File
import Ganga.Utility.logging
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import diracAPI_script_template, dirac_outputfile_jdl
from Ganga.GPIDev.Base.Proxy import isType
from GangaGaudi.Lib.Applications.Gaudi import Gaudi
logger = Ganga.Utility.logging.getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def jobid_as_string(job):
    """ Return the string representation of the job, should be same as job.getFQID() """
    jstr = ''
    if job.master:
        jstr = str(job.master.id) + os.sep + str(job.id)
    else:
        jstr = str(job.id)
    return jstr

def lhcbdirac_outputfile_jdl(output_files):
    """ Construct the setOutputFile section of the JDL for this job"""

    DiracScript = dirac_outputfile_jdl(output_files, False)

    DiracScript = DiracScript.replace('###OUTPUT_SE###', '###OUTPUT_SE###,replicate=\'###REPLICATE###\'')

    DiracScript = DiracScript.replace('outputPath', 'OutputPath').replace('outputSE', 'OutputSE')

    return DiracScript

def lhcbdiracAPI_script_template():
    """ Create a template LHCbDirac LHCbJob by taking the Dirac Job and making some modifications to teh template"""

    DiracScript = diracAPI_script_template()

    DiracLHCb_Options = """
j.setApplicationScript('###APP_NAME###', '###APP_VERSION###', '###APP_SCRIPT###', logFile='###APP_LOG_FILE###', systemConfig='###PLATFORM###')
j.setAncestorDepth(###ANCESTOR_DEPTH###)
"""


    DiracScript = DiracScript.replace('\'###EXE_LOG_FILE###\'', '\'###EXE_LOG_FILE###\', systemConfig=\'###PLATFORM###\'')
    DiracScript = DiracScript.replace('j.setPlatform( \'ANY\' )', 'j.setDIRACPlatform()')

    setName_str = 'j.setName(\'###NAME###\')'
    DiracScript = DiracScript.replace(setName_str, "%s\n%s" % (setName_str, DiracLHCb_Options))

    return DiracScript


def is_gaudi_child(app):
    """ Does this app inherit from Gaudi True/False"""
    if isType(app, Gaudi):
        return True
    return False


class filenameFilter:
    """ class for returning a given filename as a filter"""
    def __init__(self, filename):
        self.filename = filename

    def __call__(self, file):
        return file.name == self.filename


def getXMLSummaryScript(indent=''):
    '''Returns the necessary script to parse and make sense of the XMLSummary data'''
    import inspect
    from GangaLHCb.Lib.Applications.AppsBaseUtils import activeSummaryItems
    script = "###INDENT#### Parsed XMLSummary data extraction methods\n"

    for summaryItem in activeSummaryItems().values():
        script += ''.join(['###INDENT###' + line for line in inspect.getsourcelines(summaryItem)[0]])
    script += ''.join(['###INDENT###' + line for line in inspect.getsourcelines(activeSummaryItems)[0]])

    import inspect
    script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                   'XMLWorkerScript.py')

    from Ganga.GPIDev.Lib.File import FileUtils
    xml_script = FileUtils.loadScript(script_location, '###INDENT###')

    script += xml_script

    return script.replace('###INDENT###', indent)


def create_runscript(useCmake=False):
    """ Returna run script for Gaudi Apps """
    from GangaLHCb.Lib.Applications.EnvironFunctions import construct_run_environ
    environ_script = construct_run_environ(useCmake)

    import inspect
    script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                                   'WorkerScript.py')

    from Ganga.GPIDev.Lib.File import FileUtils
    worker_script = FileUtils.loadScript(script_location, '')

    worker_script = worker_script.replace('###CONSTRUCT_ENVIRON###', environ_script)

    return worker_script

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
