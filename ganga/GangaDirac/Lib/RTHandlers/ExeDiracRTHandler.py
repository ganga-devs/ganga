#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import inspect
import GangaCore.Utility.Virtualization
from GangaCore.Core.Sandbox.WNSandbox import PYTHON_DIR
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_template, diracAPI_script_settings, API_nullifier, dirac_outputfile_jdl
from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.RTHandlers.RunTimeHandlerUtils import master_sandbox_prepare, sandbox_prepare, script_generator
from GangaCore.GPIDev.Lib.File.LocalFile import LocalFile
from GangaCore.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.GPIDev.Lib.File import File, FileBuffer
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.util import unique
from GangaCore.GPIDev.Base.Proxy import isType, stripProxy
logger = getLogger()
config = getConfig('DIRAC')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class ExeDiracRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Dirac backend"""

    def master_prepare(self, app, appmasterconfig):
        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)
        if type(app.exe) == File:
            input_dir = app.getJobObject().getInputWorkspace().getPath()
            exefile = os.path.join(input_dir, os.path.basename(app.exe.name))
            if not os.path.exists(exefile):
                msg = 'Executable: "%s" must exist!' % str(exefile)
                raise ApplicationConfigurationError(None, msg)

            os.system('chmod +x %s' % exefile)
        return StandardJobConfig(inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox))

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        inputsandbox, outputsandbox = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)
        input_data,   parametricinput_data = dirac_inputdata(app)
#        outputdata,   outputdata_path      = dirac_ouputdata(app)

        job = stripProxy(app).getJobObject()
        outputfiles = [this_file for this_file in job.outputfiles if isType(this_file, DiracFile)]

        #Grab the platform if the app has that attribute
        platform = 'ANY'
        if hasattr(app, 'platform'):
            platform = app.platform

        commandline = []
        commandline.append(app.exe)
        if isType(app.exe, File):
            #logger.info("app: %s" % str(app.exe.name))
            #fileName = os.path.join(get_share_path(app), os.path.basename(app.exe.name))
            #logger.info("EXE: %s" % str(fileName))
            #inputsandbox.append(File(name=fileName))
            inputsandbox.append(app.exe)
            commandline[0]=os.path.join('.', os.path.basename(app.exe.name))
        commandline.extend([str(arg) for arg in app.args])
        logger.debug('Command line: %s: ', commandline)

        #exe_script_path = os.path.join(job.getInputWorkspace().getPath(), "exe-script.py")
        exe_script_name = 'exe-script.py'

        logger.debug("Setting Command to be: '%s'" % repr(commandline))

        contents = exe_script_template()

        virtualization = job.virtualization
        if virtualization:
            contents = virtualization.modify_script(exe_script_template(), sandbox=True)

            virtualizationutils = File(inspect.getsourcefile(GangaCore.Utility.Virtualization), subdir=PYTHON_DIR )
            inputsandbox.append(virtualizationutils)

        contents = script_generator(contents,
                                    COMMAND=repr(commandline),
                                    PYTHONDIR=repr(PYTHON_DIR),
                                    OUTPUTFILESINJECTEDCODE=getWNCodeForOutputPostprocessing(job, ''))
            
        inputsandbox.append(FileBuffer(name=exe_script_name, contents=contents, executable=True))

        logger.debug("Script is: %s" % str(contents))

        from os.path import abspath, expanduser

        for this_file in job.inputfiles:
            if isinstance(this_file, LocalFile):
                for name in this_file.getFilenameList():
                    inputsandbox.append(File(abspath(expanduser(name))))

        dirac_outputfiles = dirac_outputfile_jdl(outputfiles, config['RequireDefaultSE'])

        # NOTE special case for replicas: replicate string must be empty for no
        # replication
        dirac_script = script_generator(diracAPI_script_template(),
                                        DIRAC_IMPORT='from DIRAC.Interfaces.API.Dirac import Dirac',
                                        DIRAC_JOB_IMPORT='from DIRAC.Interfaces.API.Job import Job',
                                        DIRAC_OBJECT='Dirac()',
                                        JOB_OBJECT='Job()',
                                        NAME=mangle_job_name(app),
                                        # os.path.basename(exe_script_path),
                                        EXE=exe_script_name,
                                        # ' '.join([str(arg) for arg in app.args]),
                                        EXE_ARG_STR='',
                                        EXE_LOG_FILE='Ganga_Executable.log',
                                        ENVIRONMENT=None,  # app.env,
                                        INPUTDATA=input_data,
                                        PARAMETRIC_INPUTDATA=parametricinput_data,
                                        OUTPUT_SANDBOX=API_nullifier(outputsandbox),
                                        OUTPUTFILESSCRIPT=dirac_outputfiles,
                                        OUTPUT_PATH="",  # job.fqid,
                                        SETTINGS=diracAPI_script_settings(app),
                                        DIRAC_OPTS=job.backend.diracOpts,
                                        MIN_PROCESSORS=job.backend.minProcessors,
                                        MAX_PROCESSORS=job.backend.maxProcessors,
                                        PLATFORM=platform,
                                        REPLICATE='True' if config['ReplicateOutputData'] else '',
                                        # leave the sandbox for altering later as needs
                                        # to be done in backend.submit to combine master.
                                        # Note only using 2 #s as auto-remove 3
                                        INPUT_SANDBOX='##INPUT_SANDBOX##'
                                        )

        #logger.info("dirac_script: %s" % dirac_script)

        #logger.info("inbox: %s" % str(unique(inputsandbox)))
        #logger.info("outbox: %s" % str(unique(outputsandbox)))

        return StandardJobConfig(dirac_script,
                                 inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox))


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def exe_script_template():
    script_template = """#!/usr/bin/env python2
'''Script to run Executable application'''
import sys, os
import subprocess
from os import system, environ, pathsep, getcwd, listdir, path

runenv = environ.copy()
runenv['PATH'] = getcwd() + (pathsep + runenv['PATH'])

PYTHON_DIR = ###PYTHONDIR###
workdir = getcwd()

sys.path.insert(0,path.join(workdir, PYTHON_DIR))

execmd = ###COMMAND###

if isinstance(execmd, str):
    execmd = [execmd]

if isinstance(execmd, list):
    if path.isfile(path.abspath(execmd[0])):
        execmd[0] = path.abspath(execmd[0])

###VIRTUALIZATION###

err = None
try:
    rc = subprocess.call(execmd, env=runenv, shell=False)
except Exception as x:
    rc = -9999
    print('Exception occured in running process: ' + repr(execmd))
    print('Err was: ' + str(x))
    subprocess.call('echo $PATH', shell=True)
    print('PATH: ' + str(runenv['PATH']))
    print('PWD: ' + str(runenv['PWD']))
    print("files on WN: " + str(listdir('.')))
    raise

print("files on WN: " + str(listdir('.')))

###OUTPUTFILESINJECTEDCODE###

try:
    subprocess.call('chmod -R u+rwX  * .*', env=runenv, shell=True)
except Exception as x:
    print('Problems fixing permissions on output files to allow for clean-up.')
    print('Err was: ' + str(x))

sys.exit(rc)
"""
    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('Executable', 'Dirac', ExeDiracRTHandler)
