#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path, master_sandbox_prepare, sandbox_prepare, script_generator
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_template, diracAPI_script_settings, API_nullifier, dirac_outputfile_jdl
from GangaDirac.Lib.Files.DiracFile import DiracFile
from Ganga.GPIDev.Lib.File.LocalFile import LocalFile
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.File import File, FileBuffer
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import unique
from Ganga.GPIDev.Base.Proxy import isType, stripProxy
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

        logger.info("Setting Command to be: '%s'" % repr(commandline))

        inputsandbox.append(FileBuffer(name=exe_script_name,
                            contents=script_generator(exe_script_template(),
                                                    #remove_unreplaced = False,
                                                    # ,
                                                    COMMAND=repr(commandline),
                                                    OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')
                                                    ),
                                       executable=True))

        contents=script_generator(exe_script_template(), COMMAND=repr(commandline),
                                    OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')
                                    )

        #logger.info("Script is: %s" % str(contents))

        from os.path import abspath, expanduser

        for this_file in job.inputfiles:
            if isinstance(this_file, LocalFile):
                for name in this_file.getFilenameList():
                    inputsandbox.append(File(abspath(expanduser(name))))
            elif isinstance(this_file, DiracFile):
                name = this_file.lfn
                if isinstance(input_data, list):
                    input_data.append(name)
                else:
                    input_data = [name]

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
    script_template = """#!/usr/bin/env python
'''Script to run Executable application'''
from __future__ import print_function
from os import system, environ, pathsep, getcwd, listdir, path
import sys
import subprocess

# Main
if __name__ == '__main__':

    my_env = environ.copy()
    my_env['PATH'] = getcwd() + (pathsep + my_env['PATH'])

    exe_cmd = ###COMMAND###

    if isinstance(exe_cmd, str):
        exe_cmd = [exe_cmd]

    if isinstance(exe_cmd, list):
        if path.isfile(path.abspath(exe_cmd[0])):
            exe_cmd[0] = path.abspath(exe_cmd[0])

    err = None
    try:
        rc = subprocess.call(exe_cmd, env=my_env, shell=False)
    except Exception as x:
        rc = -9999
        print('Exception occured in running process: ' + exe_cmd)
        print('Err was: ' + str(err))
        subprocess.call('''['echo', '$PATH']''')
        print('PATH: ' + str(my_env['PATH']))
        print('PWD: ' + str(my_env['PWD']))
        print("files on WN: " + str(listdir('.')))
        raise

    print("files on WN: " + str(listdir('.')))

    ###OUTPUTFILESINJECTEDCODE###

    sys.exit(rc)
"""
    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('Executable', 'Dirac', ExeDiracRTHandler)
