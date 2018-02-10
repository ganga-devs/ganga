#!/usr/bin/env python

######################################################
# ratRunner.py
# ---------
# Author: Matt Mottram
#         <m.mottram@sussex.ac.uk>
#
# Description:
#    Runs user RAT jobs as a Ganga submitted job.
#
# Ships with all RATUser jobs.
# Allows users to install rat snapshots to the temporary
# job directory on the submission backend.  Can use a token
# or a tar.gz file that ships with the job to install.
# Script requires knowledge of the base version of RAT
# that the snapshot derives from (and creates an environment
# file accordingly).  Base version must be installed on the
# backend in a named location.
#
# Revision History:
#  - 2012/11/03: M. Mottram: first revision with proper documentation!
#  - 2014/05/19: M. Mottram: moved common methods to job_tools
#
######################################################

import os
import sys
import optparse
import glob
import socket
import json
import job_tools


#################################
# Deprecated functions
#################################
def download_local(token, run_version):
    '''Download copy of RAT.
    Should use method to ship with job instead.
    '''
    install_path = os.path.join(os.getcwd(), "rat")
    download_rat(token, run_version, install_path)
    
def download_rat(token, run_version, install_path):
    '''download the requested version of RAT, unzip it and
    move it all to a rat directory
    Should use method to ship with job instead'''
    cache_path = os.getcwd()
    url = "https://api.github.com/repos/snoplus/rat/tarball/%s" % run_version
    tar_name = 'rat.tar.gz'
    job_tools.download_file(url, token, filename=tar_name, cache_path=cache_path)
    job_tools.untar_file( tarName, installPath, 'rat', cache_path, 1 ) # Strip the first directory


#################################
# Current functions
#################################
def untar_local(tarname):
    '''Untar a repository shipped with the job'''
    install_path = os.getcwd()
    job_tools.untar_file(tarname, installPath, 'rat', os.getcwd(), 0) # nothing to strip, but do need to move
    # For some reason, configure is not x by default
    job_tools.execute('chmod', ['u+x','rat/configure'], cwd=installPath)


def install_local(run_version, env_file):
    '''Install a shipped / downloaded tar for use.
    '''
    #should be in the temporary job directory to do this
    install_path = os.path.join(os.getcwd(), "rat")
    create_env('installerTemp.sh', env_file)
    compile_rat()
    create_env('installerEnv.sh', env_file, install_path)


def create_env(filename, env_file, rat_dir=None):
    '''Create environment file to source for RAT jobs.
    
    If rat_dir is not set, then assumes only base directories are needed.
    '''
    if not os.path.exists(env_file):
        raise Exception("create_env::cannot find environment %s" % env_file)
    fin = file(env_file, 'r')
    env_text = ''
    lines = fin.readlines()
    for i,line in enumerate(lines):
        if 'source ' in line:
            if i==len(lines)-1 or i==len(lines)-2:
                # This is the fixed RAT release line; remove it!
                pass
            else:
                env_text += '%s \n' % line
        else:
            env_text += '%s \n' % line
    if rat_dir is not None:
        env_text += "source %s" % os.path.join(rat_dir, "env.sh")
    env_file = file(filename, 'w')
    env_file.write(env_text)


def compile_rat():
    '''Compile local version of RAT'''
    temp_file = os.path.join(os.getcwd(), 'installerTemp.sh')
    rat_dir   = os.path.join(os.getcwd(), 'rat')
    command_text = "\
#!/bin/bash\n \
source %s\n \
cd %s\n \
./configure \n \
source env.sh\n \
scons" % (temp_file, rat_dir)
    job_tools.execute_complex(command_text)
    #running locally, seems rat is not executable by default!
    job_tools.execute('chmod', ['u+x', 'rat/bin/rat'])


def run_rat(options):
    '''Run RAT according to the options.
    '''
    rat_options = ['-l', 'rat.log']
    if options.input_file:
        rat_options += ['-i', options.input_file]
    if options.output_file:
        rat_options += ['-o', options.output_file]
    if options.n_events:
        rat_options += ['-N', options.n_events]
    elif options.run_time:
        rat_options += ['-T', options.run_time]
    if options.dbuser and options.dbpassword and options.dbname and options.dbprotocol and options.dburl:
        rat_options += ['-b', '%s://%s:%s@%s/%s' % (options.dbprotocol, options.dbuser, options.dbpassword,
                                                    options.dburl, options.dbname)]
    rat_options += [options.rat_macro]
    job_tools.execute_ratenv('rat', rat_options)    
    print os.listdir(os.getcwd())

    
def check_outputs(options):
    '''Before running RAT, check whether output data already exists!
    '''
    # Can only check the outputs for files with output file specified in the command options
    if options.output_file is None:
        print "check_outputs::no output file specified"
        return None
    if options.grid_mode is None:
        print "check_outputs::no grid mode specified"
        return None
    # For output files, check if there is an output root and output ntuple processor in the macro
    ntuple = job_tools.check_processor(options.rat_macro, 'outntuple')
    root = job_tools.check_processor(options.rat_macro, 'outroot')
    suffixes = []
    exists = []
    if root:
        suffixes.append(".root")
        exists.append(False)
    if ntuple:
        suffixes.append(".ntuple.root")
        exists.append(False)
    all_exist = True
    any_exist = False
    if len(suffixes)==0:
        print "check_output::no files to check!"
        print suffixes
        all_exist = False
    for i, s in enumerate(suffixes):
        root_file = "%s%s" % (options.output_file, s)
        lfc_path = os.path.join('lfn:/grid/snoplus.snolab.ca', options.output_dir, root_file)
        exists[i] = False
        try:
            job_tools.exists(lfc_path)
            # lfc exists, get the replica
            exists[i] = True
            any_exist = True
            print "check_outputs::LFN already in use %s" % lfc_path
        except job_tools.JobToolsException as e:
            print "check_outputs::LFN unused %s, %s" % (lfc_path, e)
            all_exist = False
    # Now, if all exist, we need to append the return card and show that the job has in fact completed
    # If only some (i.e. any) exist, but not all, then we need to raise this problem by failing the job!
    dump_out = {}
    if all_exist:
        print "check_outputs::All expected outputs already exist on LFN"
        for s in suffixes:
            try:
                root_file = "%s%s" % (options.output_file, s)
                lfc_path = os.path.join('lfn:/grid/snoplus.snolab.ca', options.output_dir, root_file)
                guid = job_tools.getguid(lfc_path)
                replica = job_tools.listreps(lfc_path)[0]
                checksum = job_tools.checksum(replica)
                size = job_tools.getsize(replica)
                
                # Note that the actual se and replica information may be different to the local information
                se_name = replica
                if se_name.startswith('srm://'):
                    se_name = se_name[6:]
                se_name = se_name.split('/')[0] # never any /
                dump_out[root_file] = {}
                dump_out[root_file]['guid'] = guid
                dump_out[root_file]['se'] = replica
                dump_out[root_file]['size'] = size
                dump_out[root_file]['cksum'] = checksum
                dump_out[root_file]['name'] = se_name
                dump_out[root_file]['lfc'] = lfc_path
            except job_tools.JobToolsException as e:
                print "check_outputs::Problem checking output for %s: %s" % (lfc_path, e)
                raise
        return dump_out
    else:
        return None


def check_output_file(options):  
    '''Check root file for TTree entries
    '''
    # Can only check the outputs for files with output file specified in the command options
    if options.output_file is None:
        print "check_output_file::no output file specified"
        return None

    # Can only check the outputs for files with number of events specified in the command options
    if options.n_events is None:
        print "check_output_file::no number of events specified"
        return None

    # Create command to run python script to check TTree entries
    # Will raise an exception if the checks fail
    ntuple = job_tools.check_processor(options.rat_macro, 'outntuple')
    root = job_tools.check_processor(options.rat_macro, 'outroot')
    soc = job_tools.check_processor(options.rat_macro, 'outsoc')
    if root or soc:
        rtc, out, err = job_tools.execute_ratenv('python', ['check_root_output.py', '-f', '%s.root' % options.output_file,
                                                        '-n', options.n_events, '-v', options.base_version])
        if rtc==0:
            # this must be true (exception raised otherwise)
            print "Outputs file checks successful"
    if ntuple:
        rtc, out, err = job_tools.execute_ratenv('python', ['check_root_output.py', '-f', '%s.ntuple.root' % options.output_file,
                                                        '-n', options.n_events, '-v', options.base_version])
        if rtc==0:
            # this must be true (exception raised otherwise)
            print "Outputs file checks successful"


def copy_data(output_dir, grid_mode):
    '''Copy all output data (i.e. any .root output files)
    grid_mode true: use lcg-cr to copy and register file
             false: use cp (the job runs in a temp dir, any files left at end are deleted eventually)
    '''
    # Still need catch-all files here (in case ROOT splits them)
    root_files = glob.glob('*.root')
    dump_out = job_tools.copy_data(root_files, output_dir, grid_mode)
    return dump_out


if __name__ == '__main__':
    print 'ratRunner...??'
    parser = optparse.OptionParser( usage = "ganga %prog [flags]")
    parser.add_option("-t", dest="token", help="Token to use, if in token mode")
    parser.add_option("-b", dest="base_version", help="RAT base version")
    parser.add_option("-v", dest="run_version", help="RAT version to run")
    parser.add_option("-e", dest="env_file", help="Software environment file to source")
    parser.add_option("-m", dest="rat_macro", help="RAT macro to use")
    parser.add_option("-d", dest="output_dir", help="LFC/SURL relative directory if in grid mode, full dir path if not in grid mode")
    parser.add_option("-g", dest="grid_mode", default=None, help="Grid mode (allowed modes are srm and lcg)")
    parser.add_option("-f", dest="zip_filename", help="Zip filename for output file")
    parser.add_option("-i", dest="input_file", default=None, help="Specify input file")
    parser.add_option("-o", dest="output_file", default=None, help="Specify output file")
    parser.add_option("-N", dest="n_events", default=None, help="Number of events (must not be in macro)")
    parser.add_option("-T", dest="run_time", default=None, help="Duration of run (cannot use with n_events)")
    parser.add_option("--dbuser", dest="dbuser", default=None, help="Database user")
    parser.add_option("--dbpassword", dest="dbpassword", default=None, help="Database password")
    parser.add_option("--dbname", dest="dbname", default=None, help="Database name")
    parser.add_option("--dbprotocol", dest="dbprotocol", default=None, help="Database protocol (http or https)")
    parser.add_option("--dburl", dest="dburl", default=None, help="Database URL (sans protocol)")
    parser.add_option("--nostore", dest="nostore", action="store_true", help="Don't copy the outputs at the end")
    parser.add_option("--voproxy", dest="voproxy", default=None, help="VO proxy location, MUST be used with grid srm mode")

    (options, args) = parser.parse_args()
    if not options.base_version:
        print ' need base version'
    if not options.run_version:
        print 'RUNNING WITH A FIXED RELEASE'
    if not options.rat_macro:
        print ' need macro'
    if not options.output_dir:
        print ' need output directory'
    if not options.env_file:
        print ' need environment file'
    if options.grid_mode=='srm':
        if not options.voproxy:
            print 'Grid %s mode, must define voproxy' % options.grid_mode
            parser.print_help()
            raise Exception
        elif not os.path.exists(options.voproxy):
            print 'Grid %s mode, must define valid voproxy' % options.grid_mode
            parser.print_help()
            raise Exception
    if not options.base_version or not options.rat_macro or not options.output_dir or not options.env_file:
        print 'options not all present, cannot run'
        parser.print_help()
        raise Exception
    elif options.run_version and not options.token and not options.zip_filename:
        print 'must choose one of token/filename to run with a specific hash version'
        parser.print_help()
        raise Exception
    elif options.token and options.zip_filename:
        print 'must choose only one of token/filename'
        parser.print_help()
        raise Exception

    # Now we're sure that we can run

    # First, download any software and setup the environment
    # that will be used by all subsequent functions
    job_helper = job_tools.JobHelper.get_instance() # Singleton
    if options.run_version:
        if options.token:
            download_local(options.token, options.run_version)
        else:
            untar_local(options.zip_filename)
        # all args have to be strings anyway (from Ganga) - force base version into a string
        install_local(options.run_version, options.env_file)
        job_helper.add_environment(os.path.join(os.getcwd(), "installerEnv.sh"))
    else:
        # Still need to ensure that the JobHelper singleton knows about the environment
        job_helper.add_environment(options.env_file)

    # Set the default environments that are needed
    if options.grid_mode=='srm':
        job_helper.set_variables(X509_USER_PROXY=options.voproxy)

    dbAccess = None
    if options.dbuser and options.dbpassword and options.dbname and options.dbprotocol and options.dburl:
        dbAccess = {}
        dbAccess['user'] = options.dbuser
        dbAccess['password'] = options.dbpassword
        dbAccess['name'] = options.dbname
        dbAccess['protocol'] = options.dbprotocol
        dbAccess['url'] = options.dburl
    
    # Check if the output already exists
    dump_out = check_outputs(options)
    if dump_out is None:
        # We do need to run the job
        run_rat(options)
        check_output_file(options)
        dump_out = {}
        if not options.nostore:
             # the nostore option is only applied when running in testing mode for production
            dump_out = copy_data(options.output_dir, options.grid_mode)
        else:
            # return a dummy output dump - no storage information but the production scripts expect it
            dump_out = {"DATA": "DELETED"}
        return_card = file('return_card.js','w')
        return_card.write(json.dumps(dump_out))
        return_card.close()
    else:
        return_card = file('return_card.js','w')
        return_card.write(json.dumps(dump_out))
        return_card.close()
        raise Exception("RATRUNNER: requested outputs already exist, see return_card.js for details")
            
