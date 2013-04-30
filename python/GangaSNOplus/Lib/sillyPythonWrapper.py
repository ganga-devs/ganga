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
# Revision History:
#  - 03/11/12: M. Mottram: first revision with proper documentation!
#  - 12/12/12: M. Mottram: added wrapper for westgrid.
# 
######################################################

import os
import sys
import subprocess
import optparse

def runScriptLCG(scriptName,args):
    script = ''
    script += 'export LD_LIBRARY_PATH=$VO_SNOPLUS_SNOLAB_CA_SW_DIR/lib:$LD_LIBRARY_PATH\n'
    script += 'export PATH=$VO_SNOPLUS_SNOLAB_CA_SW_DIR/bin:$PATH\n'
    script += 'python %s' % scriptName
    for arg in args:
        script += ' %s' % arg
    #as long as the python command is the final one, the exception raising should work!
    ExecuteComplexCommand(os.getcwd(),script)

def runScriptWG(scriptName,args):
    script = ''
    script += 'module load application/python/2.7.3 \n'
    script += 'source /opt/exp_software/atlas.glite/setup_WN.sh \n'
    script += 'export LFC_HOST=lfc.gridpp.rl.ac.uk \n'
    script += 'export MYPROXY_SERVER=myproxy.westgrid.ca \n'
    script += 'python %s' % scriptName
    for arg in args:
        script += ' %s' % arg
    #as long as the python command is the final one, the exception raising should work!
    ExecuteComplexCommand(os.getcwd(),script)

def runScriptMisc(scriptName,envFile,args):
    script = ''
    for line in file(envFile,'r').readlines():
        script += '%s \n' % line
    script += 'python %s' % scriptName
    for arg in args:
        script += ' %s' % arg
    #as long as the python command is the final one, the exception raising should work!
    ExecuteComplexCommand(os.getcwd(),script,True)

def ExecuteComplexCommand( installPath, command, loginShell=False):
    """ Execute a multiple line bash command, writes to a temp bash file then executes it."""
    print 'installPath:',installPath
    fileName = os.path.join( installPath, "tempSilly.sh" )#give it a different name to the script in ratRunner.py
    commandFile = open( fileName, "w" )
    commandFile.write( command )
    commandFile.close()
    if loginShell==False:
        output = ExecuteSimpleCommand( "/bin/bash", [fileName], None, installPath )
    else:
        output = ExecuteSimpleCommand( "/bin/bash", ['-l',fileName], None, installPath )
    os.remove( fileName )
    return output

def ExecuteSimpleCommand( command, args, env, cwd, exitIfFail=True, verbose = False ):
    """ Blocking execute command. Returns True on success"""
    shellCommand = [ command ] + args
    useEnv = os.environ # Default to current environment
    if env is not None:
        for key in env:
            useEnv[key] = env[key]
    process = subprocess.Popen( args = shellCommand, env = useEnv, cwd = cwd, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    output = ""
    error = ""
    output, error = process.communicate()
    logText = command + output
    if process.returncode != 0:
        print 'process failed:',shellCommand
        print 'output : ',output
        print 'error  : ',error
        if exitIfFail:
            raise Exception
    output += error
    return output

if __name__=='__main__':
    parser = optparse.OptionParser()
    parser.add_option('-s',type='string',dest='script',help='python script to run')
    parser.add_option('-a',type='string',dest='args',help='python script args')
    parser.add_option('-l',type='string',dest='location',help='where am i running (lcg/wg)')
    #parser.add_option('-p',type='string',dest='proxy',help='location of a proxy (for use at wg)')
    parser.add_option('-f',type='string',dest='envFile',help='send a file with the appropriate environment')
    (options,args) = parser.parse_args()
    #just create a script that sets up the correct environment
    #and then runs the ratProdRunner script
    #... this should still return the status properly ...
    if not options.script:
        print 'no script'
    if not options.location:
        print 'no location'
    if not options.script or not options.location:
        parser.print_help()
        sys.exit(1)
    if options.location!='lcg' and options.location!='wg' \
            and options.location!='misc':
        print 'bad location:',options.location
        parser.print_help()
        sys.exit(1)
    scriptArgs = options.args.split()
    if options.location=='lcg':
        runScriptLCG(options.script,scriptArgs)
    elif options.location=='wg':
        runScriptWG(options.script,scriptArgs)
    elif options.location=='misc':
        runScriptMisc(options.script,options.envFile,scriptArgs)
