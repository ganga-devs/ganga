#!/usr/bin/env python

######################################################
# ratProdRunner.py
# ---------
# Author: Matt Mottram
#         <m.mottram@sussex.ac.uk>
#
# Description:
#    Runs production/processing as a Ganga submitted job.
#
# Ships with all RATProd jobs.
# Script expects the correct tagged RAT release to have
# been installed on the backend with snoing.  Sources 
# the appropriate snoing environment file and runs jobs
# in the job's temporary directory. 
#
# Revision History:
#  - 2012/11/03: M. Mottram: first revision with proper documentation!
#  - 2014/05/19: M. Mottram: moved many methods to job_tools
# 
######################################################

import os
import sys
import optparse
import glob
import shutil
import base64
import socket
import json
import job_tools


def setup_grid_env(options):
    '''Setup the job_helper environment and point to the correct rat environment.
    
    TODO: update to work with different os installs in cvmfs.
    '''
    job_helper = job_tools.JobHelper.get_instance()
    job_helper.add_environment(options.env_file)


def run_rat(options):
    '''Run the RAT macro
    '''
    rat_options = ['-l', 'rat.log']
    if options.dbuser and options.dbpassword and options.dbname and options.dbprotocol and options.dburl:
        rat_options += ['-b', '%s://%s:%s@%s/%s' % (options.dbprotocol, options.dbuser, options.dbpassword,
                                                    options.dburl, options.dbname)]
    rat_options += [options.rat_macro]
    # Will exit if command fails
    job_tools.execute_ratenv('rat', rat_options)


def run_script(prod_script):
    '''Just run a production script, within the rat environment (as setup by job_helper)
    '''
    job_tools.execute_ratenv('/bin/bash', prod_script)


def copy_data(output_files, output_dir, grid_mode):
    '''Copy all output data (i.e. any .root output files)
    gridMode true: use lcg-cr to copy and register file
             false: use cp (the job runs in a temp dir, any files left at end are deleted eventually)
    '''
    # comma delimited and in braces (will be as string)
    # need to remove spaces
    output_files = output_files.strip("[]").split(',')
    dump_out = job_tools.copy_data(output_files, output_dir, grid_mode)
    return dump_out


def get_data(input_files, input_dir, grid_mode):
    '''Copy the input data across
    '''
    input_files = input_files.strip("[]").split(',')
    job_tools.get_data(input_files, input_dir, grid_mode)


if __name__ == '__main__':
    parser = optparse.OptionParser( usage = "ganga %prog [flags]")
    parser.add_option("-e", dest="env_file", help="Software environment file to source")
    parser.add_option("-m", dest="rat_macro", help="RAT macro to use (or script if in shell mode)")
    parser.add_option("-d", dest="output_dir", help="LFC/SURL relative directory if in grid mode, full dir path if not in grid mode")
    parser.add_option("-g", dest="grid_mode", default=None, help="Grid mode")
    parser.add_option("-k", action="store_true", dest="shell_mode", help="Shell mode: use if passing a shell script to run")
    parser.add_option("-i", dest="input_files", help="list of input files, must be in [braces] and comma delimited")
    parser.add_option("-x", dest="input_dir", help="LFC/SURL relative directory if in grid mode, full dir path if not in grid mode")
    parser.add_option("-o", dest="output_files", help="list of output files, must be in [braces] and comma delimited")
    parser.add_option("--dbuser", dest="dbuser", default=None, help="Database user")
    parser.add_option("--dbpassword", dest="dbpassword", default=None, help="Database password")
    parser.add_option("--dbname", dest="dbname", default=None, help="Database name")
    parser.add_option("--dbprotocol", dest="dbprotocol", default=None, help="Database protocol (http or https)")
    parser.add_option("--dburl", dest="dburl", default=None, help="Database URL (sans protocol)")
    parser.add_option("--nostore", dest="nostore", action="store_true", help="Don't copy the outputs at the end")
    parser.add_option("--voproxy", dest="voproxy", default=None, help="VO proxy location, MUST be used with grid srm mode")
    (options, args) = parser.parse_args()
    if not options.rat_macro or not options.output_dir or not options.env_file and not options.output_files:
        print 'options not all present'
    else:
        if options.input_files:
            if not options.input_dir:
                raise Exception('No input directory specified')
            get_data(options.input_dir, options.input_files, options.grid_mode, options.voproxy)
        if options.grid_mode=='srm':
            if not options.voproxy:
                parser.print_help()
                raise Exception('Grid %s mode, must define voproxy' % options.grid_mode)
            elif not os.path.exists(options.voproxy):
                parser.print_help()
                raise Exception('Grid %s mode, must define valid voproxy' % options.grid_mode)
        # Setup the correct environment        
        job_helper = job_tools.JobHelper.get_instance() # Singleton
        job_helper.add_environment(options.env_file)
        if options.shell_mode:
            # Assumes a bash script!
            run_script(options.rat_macro)
        else:            
            run_rat(options)
        dump_out = {}
        if not options.nostore:
            #the nostore option is only applied when running in testing mode for production            
            dump_out = copy_data(options.output_files, options.output_dir, options.grid_mode)
        else:
            #return a dummy output dump - no storage information but the production scripts expect it
            dump_out = {"DATA": "DELETED"}
        return_card = file('return_card.js', 'w')
        return_card.write(json.dumps(dump_out))
        return_card.close()
