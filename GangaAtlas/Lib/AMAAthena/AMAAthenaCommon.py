###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AMAAthenaCommon.py,v 1.5 2009-05-11 16:33:56 hclee Exp $
###############################################################################
# Common utilities for AMAAthena specific taskes
#
# NIKHEF/ATLAS
#

import re
import os
import os.path
import tempfile

from Ganga.Core.exceptions import ApplicationConfigurationError

from Ganga.Utility.logging import getLogger
logger = getLogger()

## copies the job option file from release or user area to job's inputdir
def get_option_files(files=[], wdir=os.getcwd()):
    '''copies the job option files into a given wdir'''


    option_files = []

    ec = -999
    stdout = ''
    stderr = ''

    for f in files:
        cmd = 'get_joboptions %s' % f

        (ec, stdout, stderr) = execSyscmdSubprocess(cmd, wdir=wdir)

        if ec != 0:
            raise ApplicationConfigureError('Failed to get job option %s: %s' % (f,stderr) )
        else:
            option_files += [ os.path.join(wdir, f) ]
            continue

    return option_files


## gets/resolves the summary tarball file name
def get_summary_lfn(job):
    '''gets/resolves the summary tarball file name'''

    summary_lfn = 'ama_summary.tgz'

    return summary_lfn

## gets/generates sample name from the job
def get_sample_name(job):
    '''gets/generates the sample name from the job's attributes'''
    
    return job.application.sample_name

## system command executor with subprocess
def execSyscmdSubprocess(cmd, wdir=os.getcwd()):
    '''executes system command vir subprocess module'''

    import subprocess

    exitcode = -999

    mystdout = ''
    mystderr = ''

    try:

        ## resetting essential env. variables
        my_env = os.environ

        if my_env.has_key('LD_LIBRARY_PATH_ORIG'):
            my_env['LD_LIBRARY_PATH'] = my_env['LD_LIBRARY_PATH_ORIG']

        if my_env.has_key('PATH_ORIG'):
            my_env['PATH'] = my_env['PATH_ORIG']

        if my_env.has_key('PYTHONPATH_ORIG'):
            my_env['PYTHONPATH'] = my_env['PYTHONPATH_ORIG']

        child = subprocess.Popen(cmd, cwd=wdir, env=my_env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        (mystdout, mystderr) = child.communicate()

        exitcode = child.returncode

    finally:
        pass

    return (exitcode, mystdout, mystderr)

# function for converting AMA configurations into Athena job option
def ama_config_to_joption(config_file, option_file, flags):
    '''converts ama configuration to job option by calling an external helper script'''

    isDone = False

    os.environ['LD_LIBRARY_PATH_ORIG'] = os.environ['LD_LIBRARY_PATH']
    os.environ['PYTHONPATH_ORIG'] = os.environ['PYTHONPATH']
    os.environ['PATH_ORIG'] = os.environ['PATH']

    cmd  = 'AMAConfigfileConverter '
    cmd += config_file + ' ' + option_file + ' ' + ' '.join(flags)

    logger.debug(cmd)

    (exitcode, myout, myerr) = execSyscmdSubprocess(cmd)

    if exitcode == 0 and os.path.exists(option_file):
        isDone = True
    else:
        logger.debug(myout)
        logger.error(myerr)
        isDone = False

    return isDone

def ama_make_config_joption(job, joption_fpath=None):
    '''generates AMA configuration job option'''

    app = job.application

    ama_config_file = app.driver_config.config_file
    ama_flags       = re.split('[\s|:]+', app.driver_flags)
    ama_sample_name = get_sample_name(job)

    ## remove empty flags
    try:
        ama_flags.remove('')
    except ValueError:
        pass

    ama_config_opttmp  = tempfile.mktemp(suffix='AMAConfig_jobOptions')

    ama_config_optfile = joption_fpath
    if not ama_config_optfile:
        ama_config_optfile = os.path.join(job.inputdir, 'AMAConfig_jobOptions.py')

    logger.debug('converting AMA configuration to Athena job option file')

    if ama_config_to_joption(ama_config_file.name, ama_config_opttmp, ama_flags):

        if os.path.exists( ama_config_opttmp ):

            try:
                ## modify the ama_config_opttmp and create the final ama_config_optfile in job's workdir
                f = open( ama_config_optfile , 'w' )
                f.write( 'SampleName = \'%s\'\n' % ama_sample_name )
                f.write( 'ConfigFile = \'%s\'\n' % os.path.basename(ama_config_file.name) )
                f.write( 'FlagList = \'%s\'\n' % ' '.join(ama_flags) )
                f.write( 'EvtMax = %d\n' % app.max_events )
                f.write( 'AMAAthenaFlags = %s\n' % repr(app.job_option_flags) )

                ft = open( ama_config_opttmp, 'r' )

                while True:
                    d = ft.read( 8096 )

                    if not d:
                        break
                    else:
                        f.write(d)

                ft.close()
                f.close()

            finally:
                os.remove( ama_config_opttmp )
    else:
        raise ApplicationConfigurationError('Failed to convert AMA configuration to job option file')

    return ama_config_optfile