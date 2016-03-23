##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Executable.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
##########################################################################

from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import File, ShareDir, FileBuffer
from Ganga.Core import ApplicationConfigurationError, ApplicationPrepareError

from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Base.Proxy import getName, isType, stripProxy

import os
import shutil
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path, master_sandbox_prepare, sandbox_prepare, script_generator
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_template, diracAPI_script_settings, API_nullifier, dirac_outputfile_jdl
from GangaDirac.Lib.Files.DiracFile import DiracFile
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Utility.util import unique

logger = getLogger()

class GaudiRunRTHandler(IRuntimeHandler):

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):

        print("app: %s" % app)
        print("appconf: %s" % appconfig)
        print("appmasterconfig: %s" % str(appmasterconfig))
        print("jobmasterconfig: %s" % str(jobmasterconfig))

        prepared_files = app.get_prepared_files()

        job_command = 'ls'
        job_args = ['-l']
        #input_sand = stripProxy(app).getJobObject().inputsandbox
        output_sand = stripProxy(app).getJobObject().outputsandbox

        input_sand = prepared_files

        c = StandardJobConfig(job_command, input_sand, job_args, output_sand)
        return c

allHandlers.add('GaudiRun', 'LSF', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'Local', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'PBS', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'SGE', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'Condor', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'TestSubmitter', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'Interactive', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'Batch', GaudiRunRTHandler)
allHandlers.add('GaudiRun', 'Cronus', GaudiRunRTHandler)

class GaudiRunDiracRTHandler(IRuntimeHandler):

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

        job = stripProxy(app).getJobObject()
        outputfiles = [this_file for this_file in job.outputfiles if isType(this_file, DiracFile)]

        commandline = app.exe
        if isType(app.exe, File):
            inputsandbox.append(app.exe)
            commandline = os.path.basename(app.exe.name)
        commandline += ' '
        commandline += ' '.join([str(arg) for arg in app.args])
        logger.debug('Command line: %s: ', commandline)

        exe_script_name = 'exe-script.py'

        inputsandbox.append(FileBuffer(name=exe_script_name,
                            contents=script_generator(exe_script_template(),
                                                    COMMAND=commandline,
                                                    OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')
                                                    ),
                                       executable=True))

        dirac_outputfiles = dirac_outputfile_jdl(outputfiles)

        # NOTE special case for replicas: replicate string must be empty for no
        # replication
        dirac_script = script_generator(diracAPI_script_template(),
                                        DIRAC_IMPORT='from DIRAC.Interfaces.API.Dirac import Dirac',
                                        DIRAC_JOB_IMPORT='from DIRAC.Interfaces.API.Job import Job',
                                        DIRAC_OBJECT='Dirac()',
                                        JOB_OBJECT='Job()',
                                        NAME=mangle_job_name(app),
                                        EXE=exe_script_name,
                                        EXE_ARG_STR='',
                                        EXE_LOG_FILE='Ganga_Executable.log',
                                        ENVIRONMENT=None,  # app.env,
                                        INPUTDATA=input_data,
                                        PARAMETRIC_INPUTDATA=parametricinput_data,
                                        OUTPUT_SANDBOX=API_nullifier(outputsandbox),
                                        OUTPUTFILESSCRIPT=dirac_outputfiles,
                                        OUTPUT_PATH="",  # job.fqid,
                                        OUTPUT_SE=getConfig('DIRAC')['DiracOutputDataSE'],
                                        SETTINGS=diracAPI_script_settings(app),
                                        DIRAC_OPTS=job.backend.diracOpts,
                                        REPLICATE='True' if getConfig('DIRAC')['ReplicateOutputData'] else '',
                                        # leave the sandbox for altering later as needs
                                        # to be done in backend.submit to combine master.
                                        # Note only using 2 #s as auto-remove 3
                                        INPUT_SANDBOX='##INPUT_SANDBOX##'
                                        )

        return StandardJobConfig(dirac_script,
                                 inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox))


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def exe_script_template():
    script_template = """#!/usr/bin/env python
'''Script to run Executable application'''
from os import system, environ, pathsep, getcwd
import sys

# Main
if __name__ == '__main__':

    environ['PATH'] = getcwd() + (pathsep + environ['PATH'])        
    rc = (system('###COMMAND###')/256)

    ###OUTPUTFILESINJECTEDCODE###
    sys.exit(rc)
"""
    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('GaudiRun', 'Dirac', GaudiRunDiracRTHandler)
