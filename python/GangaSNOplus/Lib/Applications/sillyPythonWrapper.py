#!/usr/bin/env python

######################################################
# sillyPythonWrapper.py
# ---------
# Author: Matt Mottram
#         <m.mottram@sussex.ac.uk>
#
# Description:
#    Sets up the correct python for SNO+ LCG jobs.
#
# Not really a wrapper, but required for ratProdRunner and
# ratRunner jobs that are submitted to the LCG.  RAT needs
# python 2.6 or later (installed in the VO_SNOPLUS_SNOLAB_CA_SW_DIR)
# but LCG backends default to python 2.4.3. 
#
# Importantly: functions in this script must work on older (e.g. 2.4)
# versions of python.
#
# Revision History:
#  - 03/11/12: M. Mottram: first revision with proper documentation!
#  - 12/12/12: M. Mottram: added wrapper for westgrid.
#  - 26/08/14: M. Mottram: updated for use with CVMFS; pep8
# 
######################################################

import os
import sys
import subprocess
import optparse
import job_tools

def check_el_version():
    '''Test the Enterprise Linux version.'''
    import platform
    # Could also check /etc/system-release
    bits = platform.release().split('.')
    release = None
    for b in bits:
        if "el" in b:
            try:
                release = int(b.strip('el'))
            except (ValueError, TypeError):
                print "Unknown system: " + platform.release()
                raise
    if release==5 or release==6:
        return release
    # Arrived here: do not know the system
    raise Exception("Unknown system: "+platform.release())


def run_script_lcg(script_name, args):
    '''Setup environment and run script on LCG worker node.'''
    version = check_el_version()
    # Only needed if CVMFS software is being used
    script = 'source $VO_SNOPLUS_SNOLAB_CA_SW_DIR/sl' + str(version) + '/env_cvmfs.sh\n'
    # hack on next line; uncomment for sites where VO_SNOPLUS_SNOLAB_CA_SW_DIR does not point to cvmfs
    # script = 'source /cvmfs/snoplus.gridpp.ac.uk/sl' + str(version) + '/env_cvmfs.sh\n'
    script += 'python %s ' % script_name
    script += ' '.join(str(a) for a in args)
    # As long as the python command is the final one, the exception raising should work!
    job_tools.execute_complex(script, os.getcwd(), True)


def run_script_wg(script_name, args):
    '''Setup environment and run script on WG worker node.'''
    script = 'module load application/python/2.7.3 \n'
    # Unsure if any of these are still required
    script += 'source /opt/exp_software/atlas.glite/setup_WN.sh \n'
    script += 'export LFC_HOST=lfc.gridpp.rl.ac.uk \n'
    script += 'export MYPROXY_SERVER=myproxy.westgrid.ca \n'
    script += 'python %s ' % script_name
    script += ' '.join(str(a) for a in args)
    # As long as the python command is the final one, the exception raising should work!
    job_tools.execute_complex(script, os.getcwd(), True)


def run_script(script_name, args, env_file):
    '''Setup environment and run script for an arbitrary backend.

    Knowledge of environment is required.'''
    script = file(env_file, 'r').read()
    script += '\n' # Just in case no eof
    script += 'python %s ' % script_name
    script += ' '.join(str(a) for a in args)
    # As long as the python command is the final one, the exception raising should work!
    job_tools.execute_complex(script, os.getcwd(), True, True)


if __name__=='__main__':
    parser = optparse.OptionParser("usage: <options> script location")
    parser.add_option('-s', type='string', dest='script', help='python script to run')
    parser.add_option('-a', type='string', default='', dest='args', help='python script args')
    parser.add_option('-l', type='string', dest='location', help='where am i running (lcg/wg)')
    parser.add_option('-f', type='string', dest='env_file', help='send a file with the appropriate environment')
    (options, args) = parser.parse_args()

    try:
        script = args[0]
        location = args[1]
    except IndexError:
        parser.print_help()
        raise

    script_args = options.args.split() # passed a string (e.g. "-s foo -t bar")
    if location=='lcg':
        run_script_lcg(script, script_args)
    elif location=='wg':
        run_script_wg(script, script_args)
    elif location=='misc':
        run_script(script, options.env_file, script_args)
    else:
        parser.print_help()
        print 'unknown lcoation:', location
        sys.exit(1)
