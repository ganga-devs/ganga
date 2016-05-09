#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path, master_sandbox_prepare, sandbox_prepare, script_generator
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import dirac_inputdata, dirac_ouputdata, mangle_job_name, diracAPI_script_template, diracAPI_script_settings, API_nullifier, dirac_outputfile_jdl
from GangaDirac.Lib.Files.DiracFile import DiracFile
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns, getWNCodeForOutputPostprocessing
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.GPIDev.Lib.File.File import File
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.util import unique
from Ganga.GPIDev.Base.Proxy import isType, stripProxy
logger = getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class Im3ShapeDiracRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Dirac backend"""

    def master_prepare(self, app, appmasterconfig):
        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)
        return StandardJobConfig(inputbox=unique(inputsandbox),
                outputbox=unique(outputsandbox))

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        inputsandbox, outputsandbox = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)
        input_data,   parametricinput_data = dirac_inputdata(app, hasOtherInputData=True)

        job = stripProxy(app).getJobObject()
        outputfiles = [this_file for this_file in job.outputfiles if isType(this_file, DiracFile)]

        exe_script_name = 'im3shape-script.py'

        im3shape_args = ' '.join([ os.path.basename(job.inputdata[0].lfn), os.path.basename(app.ini_location.namePattern) # input.fz, config.ini
                                   app.catalog, os.path.basename(job.inputdata[0].lfn) + '.' + app.rank + '.' + app.size, # catalog, output
                                   app.rank, app.size ])

        inputsandbox.append(FileBuffer( name=exe_script_name,
                                        contents=script_generator(Im3Shape_script_template(),
                                                                  ## ARGS for app from job.app
                                                                  EXE_NAME = app.exe_name,
                                                                  RUN_DIR = app.run_dir,
                                                                  FZ_FILE = os.path.basename(job.inputdata[0].lfn),
                                                                  INI_FILE = os.path.basename(app.ini_location.namePattern),
                                                                  BLACKLIST = os.path.basename(app.blacklist.namePattern),
                                                                  EXE_ARGS = im3spahe_args,
                                                                  ## Stuff for Ganga
                                                                  UTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    '),
                                                                 ),
                                        executable=True)
                            )

        app_file_list = [app.im3_location, app.ini_location, app.blacklist]
        
        dirac_outputfiles = dirac_outputfile_jdl(outputfiles, False)

        job.inputfiles.extend(app_file_list)

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
                ENVIRONMENT=None,
                INPUTDATA=input_data,
                PARAMETRIC_INPUTDATA=parametricinput_data,
                OUTPUT_SANDBOX=API_nullifier(outputsandbox),
                OUTPUTFILESSCRIPT=dirac_outputfiles,
                OUTPUT_PATH="",  # job.fqid,
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


def Im3Shape_script_template():
    script_template = """#!/usr/bin/env python
'''Script to run Executable application'''
from __future__ import print_function
from os import system, environ, pathsep, getcwd, listdir, path, chdir
import sys
import shutil
import subprocess

def run_Im3ShapeApp(my_env):

    run_dir = '###RUN_DIR###'
    run_dir = path.join(getcwd(), run_dir)

    INI_FILE_ = '###INI_FILE###'
    BLACKLIST_ = '###BLACKLIST###'

    shutil.move(path.join(getcwd(), INI_FILE_), run_dir)
    shutil.move(path.join(getcwd(), BLACKLIST_), path.join(run_dir, 'blacklist-y1.txt'))

    FZ_FILE_ = '###FZ_FILE###'
    FZ_FILE_ = path.join(getcwd(), FZ_FILE_)

    chdir(run_dir)

    EXE_NAME_ = '###EXE_NAME###'
    EXE_NAME_ = path.join(getcwd(), EXE_NAME_)

    EXE_ARGS_ = '###EXE_ARGS###'

    full_cmd = EXE_NAME_ + ' ' + EXE_ARGS

    print("full_cmd: %s" % full_cmd)

    rc = 0
    return rc

# Main
if __name__ == '__main__':

    my_env = environ
    my_env['PATH'] = getcwd() + (pathsep + my_env['PATH'])

    err = None
    try:
        rc = run_Im3ShapeApp(my_env)
    except Exception as x:
        rc = -9999
        print('Exception occured in running app.')
        print('Err was: ' + str(x))
        #subprocess.call('''['echo', '$PATH']''')
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
allHandlers.add('Im3ShapeApp', 'Dirac', Im3ShapeDiracRTHandler)

