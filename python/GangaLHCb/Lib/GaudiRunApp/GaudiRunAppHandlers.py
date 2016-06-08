##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Executable.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
##########################################################################

from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File.File import File, ShareDir
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
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

def genDataFiles(job):
    """
    Generating a data.py file which contains the data we want gaudirun to use
    """
    logger.debug("Doing XML Catalog stuff")

    inputsandbox = []

    data = job.inputdata
    data_str = ''
    if data:
        logger.debug("Returning options String")
        data_str = data.optionsString()
        if data.hasLFNs():
            logger.info("Generating Data catalog for job: %s" % job.fqid)
            logger.debug("Returning Catalogue")
            inputsandbox.append(FileBuffer('catalog.xml', data.getCatalog()))
            cat_opts = '\nfrom Gaudi.Configuration import FileCatalog\nFileCatalog().Catalogs = ["xmlcatalog_file:catalog.xml"]\n'
            data_str += cat_opts

        inputsandbox.append(FileBuffer('data.py', data_str))

    return inputsandbox

def generateWNScript(commandline, job):
    """
    Generate the script as a file buffer and return it
    """
    exe_script_name = 'gaudiRun-script.py'

    return FileBuffer(name=exe_script_name, contents=script_generator(exe_script_template(), COMMAND=commandline,
                                                                    OUTPUTFILESINJECTEDCODE = getWNCodeForOutputPostprocessing(job, '    ')),
                      executable=True)

def collectPreparedFiles(app):
    """
    Collect the files from the Application in the prepared state
    """
    if not isinstance(app.is_prepared, ShareDir):
        raise ApplicationConfigurationError('Failed to prepare Application Correctly')
    shared_dir = app.getSharedPath()
    input_files, input_folders = [], []
    for root, dirs, files in os.walk(shared_dir, topdown=True):
        for name in files:
            input_files.append(os.path.join(root, name))
        for name in dirs:
            input_folders.append(os.path.join(root, name))
    return input_files, input_folders


class GaudiRunRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Local backend"""

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        """
        Prepare the job in order to submit to the Local backend
        """

        job = app.getJobObject()

        prepared_files = []

        job_command = 'ls'
        job_args = ['-l']
        input_sand = job.inputsandbox
        output_sand = job.outputsandbox

        data_files = genDataFiles(job)

        input_sand = unique(input_sand + prepared_files + data_files)

        scriptToRun = generateWNScript('ls -l', job)
        input_sand.append(scriptToRun)

        
        #input_sand.append(job.application)

        input_files, input_folders = collectPreparedFiles(app)

        if input_folders:
            raise ApplicationConfigurationError('Prepared folders not supported yet, please fix this in future')
        else:
            for f in input_files:
                input_sand.append(File(f))

        print("command: %s" % scriptToRun.name)
        print("Args: %s" % [])

        c = StandardJobConfig(scriptToRun.name, input_sand, [], output_sand)
        return c

allHandlers.add('GaudiRun', 'Local', GaudiRunRTHandler)
#allHandlers.add('GaudiRun', 'Condor', GaudiRunRTHandler)
#allHandlers.add('GaudiRun', 'Interactive', GaudiRunRTHandler)
#allHandlers.add('GaudiRun', 'Batch', GaudiRunRTHandler)

class GaudiRunDiracRTHandler(IRuntimeHandler):

    """The runtime handler to run plain executables on the Dirac backend"""

    #def master_prepare(self, app, appmasterconfig):
    #    inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig)
    #    if type(app.exe) == File:
    #        input_dir = app.getJobObject().getInputWorkspace().getPath()
    #        exefile = os.path.join(input_dir, os.path.basename(app.exe.name))
    #        if not os.path.exists(exefile):
    #            msg = 'Executable: "%s" must exist!' % str(exefile)
    #            raise ApplicationConfigurationError(None, msg)

    #        os.system('chmod +x %s' % exefile)
    #    return StandardJobConfig(inputbox=unique(inputsandbox),
    #                             outputbox=unique(outputsandbox))

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):
        """
        Prepare the job in order to submit to the Dirac backend
        """
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

        scriptToRun = generateWNScript(commandline, job)
        inputfiles.append(scriptToRun)

        dirac_outputfiles = dirac_outputfile_jdl(outputfiles)

        # NOTE special case for replicas: replicate string must be empty for no
        # replication
        dirac_script = script_generator(diracAPI_script_template(),
                                        DIRAC_IMPORT='from DIRAC.Interfaces.API.Dirac import Dirac',
                                        DIRAC_JOB_IMPORT='from DIRAC.Interfaces.API.Job import Job',
                                        DIRAC_OBJECT='Dirac()',
                                        JOB_OBJECT='Job()',
                                        NAME=mangle_job_name(app),
                                        EXE=scriptToRun.name,
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
from __future__ import print_function
from os import listdir, system, environ, pathsep, getcwd
from mimetypes import guess_type
from contextlib import closing
import tarfile
import sys

def extractAllTarFiles(path):
    for f in listdir(path):
        print("examining: %s" % f )
        if guess_type(f)[1] in ['gzip', 'bzip2']:
            with closing(tarfile.open(f, "r:*")) as tf:
                print("Extracting: %s" % tf.name)
                tf.extractall('.')

# Main
if __name__ == '__main__':

    extractAllTarFiles('.')

    rc = system('###COMMAND###')

    print("hello")


    ###OUTPUTFILESINJECTEDCODE###
    sys.exit(rc)
"""
    return script_template

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('GaudiRun', 'Dirac', GaudiRunDiracRTHandler)

