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
logger = getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class Im3ShapeDiracRTHandler(IRuntimeHandler):

    """The runtime handler to run the Im3ShapeApp on the Dirac backend"""

    def master_prepare(self, app, appmasterconfig):
        """
        This function prepares the application of a master job during submit. A priori we aren't doing anything with this in Im3ShapeApp but until this is understood I'd rather not remove it
        Args:
            app (IApplication): This is the application given in the master job
            appasterconfig (tuple): This is the configuration which is to prepare the app in the master job # TODO check type and this interface
        """
        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)
        return StandardJobConfig(inputbox=unique(inputsandbox),
                outputbox=unique(outputsandbox))

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        """
        This function prepares the application of the actual job being submitted, master or not
        Args:
            app (IApplication): This is the application actually being submitted belonging to the master or sub job being configured
            appsubconfig (tuple): This is used to prepare the inputsandbox according to the configuration for each subjob if it varies
            appmasterconfig (tuple): This is also used to prepare the inputsandbox but contains the config of the app for the master job
            jobmasterconfig (StandardJobConfig): This is the configuration of the master job which may or may not be the same job as owning the app
        """

        # Construct some common objects used in job submission here
        inputsandbox, outputsandbox = sandbox_prepare(app, appsubconfig, appmasterconfig, jobmasterconfig)
        input_data,   parametricinput_data = dirac_inputdata(app, hasOtherInputData=True)


        job = app.getJobObject()

        # Construct the im3shape-script which is used by this job. i.e. the script and full command line to be used in this job
        exe_script_name = 'im3shape-script.py'
        output_filename = os.path.basename(job.inputdata[0].lfn) + '.' + str(app.rank) + '.' + str(app.size)
        im3shape_args = ' '.join([ os.path.basename(job.inputdata[0].lfn), os.path.basename(app.ini_location.namePattern), # input.fz, config.ini
                                   app.catalog, output_filename, # catalog, output
                                   str(app.rank), str(app.size) ])

        full_cmd = app.exe_name + ' ' + im3shape_args

        outputfiles = [this_file for this_file in job.outputfiles if isinstance(this_file, DiracFile)]

        inputsandbox.append(FileBuffer( name=exe_script_name,
                                        contents=script_generator(Im3Shape_script_template(),
                                                                  ## ARGS for app from job.app
                                                                  RUN_DIR = app.run_dir,
                                                                  BLACKLIST = os.path.basename(app.blacklist.namePattern),
                                                                  COMMAND = full_cmd,
                                                                  ## Stuff for Ganga
                                                                  OUTPUTFILES = repr([this_file.namePattern for this_file in job.outputfiles]),
                                                                  OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    '),
                                                                 ),
                                        executable=True)
                            )

        # TODO once there is a common, IApplication.getMeFilesForThisApp function replace this list with a getter ad it shouldn't really be hard-coded
        app_file_list = [app.im3_location, app.ini_location, app.blacklist]

        app_file_list = [this_file for this_file in app_file_list if isinstance(this_file, DiracFile)]
        job.inputfiles.extend(app_file_list)

        # Slightly mis-using this here but it would be nice to have these files
        #job.inputfiles.extend(job.inputdata)

        # NOTE special case for replicas: replicate string must be empty for no
        # replication
        dirac_script = script_generator(diracAPI_script_template(),
                DIRAC_IMPORT = 'from DIRAC.Interfaces.API.Dirac import Dirac',
                DIRAC_JOB_IMPORT = 'from DIRAC.Interfaces.API.Job import Job',
                DIRAC_OBJECT = 'Dirac()',
                JOB_OBJECT = 'Job()',
                NAME = mangle_job_name(app),
                EXE = exe_script_name,
                EXE_ARG_STR = '',
                EXE_LOG_FILE = 'Ganga_Executable.log',
                ENVIRONMENT = None,
                INPUTDATA = input_data,
                PARAMETRIC_INPUTDATA = parametricinput_data,
                OUTPUT_SANDBOX = API_nullifier(outputsandbox),
                OUTPUTFILESSCRIPT = dirac_outputfile_jdl(outputfiles, False),
                OUTPUT_PATH = "",  # job.fqid,
                SETTINGS = diracAPI_script_settings(app),
                DIRAC_OPTS = job.backend.diracOpts,
                REPLICATE = 'True' if getConfig('DIRAC')['ReplicateOutputData'] else '',
                # leave the sandbox for altering later as needs
                # to be done in backend.submit to combine master.
                # Note only using 2 #s as auto-remove 3
                INPUT_SANDBOX = '##INPUT_SANDBOX##'
                )


        return StandardJobConfig(dirac_script,
                inputbox=unique(inputsandbox),
                outputbox=unique(outputsandbox))


        #\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def Im3Shape_script_template():
    """
    This function returns the script to be run on the worker nodes for Im3ShapeApp
    """
    script_template = """#!/usr/bin/env python
'''Script to run Im3Shape application'''
from __future__ import print_function
from os import system, environ, pathsep, getcwd, listdir, path, chdir
from shutil import move
from glob import glob
from subprocess import call
from sys import exit
from copy import deepcopy

def run_Im3ShapeApp():

    # Some useful paths to store
    wn_dir = getcwd()
    run_dir = '###RUN_DIR###'
    run_dir = path.join(wn_dir, run_dir)

    ## Move all needed files into run_dir

    #Blacklist is currently hard-coded lets move the file on the WN
    blacklist_file = '###BLACKLIST###'
    move(path.join(wn_dir, blacklist_file), path.join(run_dir, 'blacklist-y1.txt'))

    # Move all .txt, .ini and .fz files in the WN to the run_dir of the executable by convention
    for pattern in ['./*.txt', './*.fz', './*.ini']:
        for this_file in glob(pattern):
            move(path.join(wn_dir, this_file), run_dir)

    ## Fully construct the command we're about to run

    chdir(run_dir)

    full_cmd = '###COMMAND###'

    print("full_cmd: %s" % full_cmd)

    my_env = deepcopy(environ)
    my_env['PATH'] = getcwd() + ':' + my_env['PATH']

    rc = call(full_cmd, env=my_env, shell=True)

    output_filePattern = ###OUTPUTFILES###

    for pattern in output_filePattern: 
        for this_file in glob(pattern):
            try:
                move(path.join(run_dir, this_file), wn_dir)
            except:
                # Let the job fail at a later stage if 1 of the outputs are missing,
                # don't crash, we may still be returning useful stuff
                pass

    print("files in run_dir: " + str(listdir('.')))

    chdir(wn_dir)

    return rc

# Main
if __name__ == '__main__':

    err = None
    try:
        rc = run_Im3ShapeApp()
    except Exception as x:
        rc = -9999
        print('Exception occured in running app.')
        print('Err was: ' + str(x))
        print("files on WN: " + str(listdir('.')))
        print("environ: %s" % environ)
        raise

    print("files on WN: " + str(listdir('.')))

    ###OUTPUTFILESINJECTEDCODE###

    exit(rc)
"""
    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('Im3ShapeApp', 'Dirac', Im3ShapeDiracRTHandler)

