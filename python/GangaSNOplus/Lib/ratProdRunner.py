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
#  - 03/11/12: M. Mottram: first revision with proper documentation!
# 
######################################################

import os
import optparse
import subprocess
import urllib2
import fnmatch
import tarfile
import zipfile
import shutil
import base64
import zlib
import socket
import json

def adler32(fileName):
    adlerBlock = 32*1024*1024
    val = 1
    f = open(fileName,'rb')
    while True:
        line = f.read(adlerBlock)
        if len(line) == 0:
            break
        val = zlib.adler32(line, val)
        if val < 0:
            val += 2**32
    f.close()
    return hex(val)[2:10].zfill(8).lower()

def ExecuteComplexCommand( installPath, command , exitIfFail=True ):
    """ Execute a multiple line bash command, writes to a temp bash file then executes it."""
    print 'installPath:',installPath
    fileName = os.path.join( installPath, "temp.sh" )
    commandFile = open( fileName, "w" )
    commandFile.write( command )
    commandFile.close()
    rtc,out,err = ExecuteSimpleCommand( "/bin/bash", [fileName], None, installPath , exitIfFail )
    os.remove( fileName )
    return rtc,out,err

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
    output = output.split('\n')#want lists
    error = error.split('\n')#will always have a '' as last element, unless :
    if output[len(output)-1]=='':
        del output[-1]
    if error[len(error)-1]=='':
        del error[-1]
    return process.returncode,output,error #don't always fail on returncode!=0

def runRat( ratMacro , ratV , swDir , gridMode):
    '''Run the RAT macro
    '''
    command = ''
    if gridMode is not None:
        #need the correct python version
        command += 'export PATH=$VO_SNOPLUS_SNOLAB_CA_SW_DIR/bin:$PATH \n'
        command += 'export LD_LIBRARY_PATH=$VO_SNOPLUS_SNOLAB_CA_SW_DIR/lib:$LD_LIBRARY_PATH \n'
    command += 'source %s \n' % (os.path.join(swDir,'env_rat-%s.sh'%ratV))
    command += 'rat -l rat.log %s \n' % ratMacro
    ExecuteComplexCommand(os.getcwd() , command)

def runScript( prodScript , ratV , swDir , gridMode):
    '''Create (another!) temporary production script
    '''
    command = ''
    if gridMode is not None:
        #need the correct python version
        command += 'export PATH=$VO_SNOPLUS_SNOLAB_CA_SW_DIR/bin:$PATH \n'
        command += 'export LD_LIBRARY_PATH=$VO_SNOPLUS_SNOLAB_CA_SW_DIR/lib:$LD_LIBRARY_PATH \n'
    command += 'source %s \n' % (os.path.join(swDir,'env_rat-%s.sh'%ratV))
    for line in file(prodScript,'r').readlines():
        command += (line + '\n')
    ExecuteComplexCommand(os.getcwd() , command)

def copyData(outputDir,outputFiles,gridMode,voproxy,myproxy):
    '''Copy all output data (i.e. any .root output files)
    gridMode true: use lcg-cr to copy and register file
             false: use cp (the job runs in a temp dir, any files left at end are deleted eventually)
    '''
    #comma delimited and in braces (will be as string)
    #need to remove spaces
    outputFiles = outputFiles.strip("[]").split(',')
    dumpOut = {}
    if gridMode is not None:
        if gridMode == 'lcg':
            lfcDir = os.path.join('lfn:/grid/snoplus.snolab.ca',outputDir)
            ExecuteSimpleCommand('lfc-mkdir',[lfcDir.lstrip('lfn:')],None,os.getcwd(),False)
            for fout in outputFiles:
                fout = fout.strip()#remove any spaces or other junk
                command = 'lcg-cr'
                sePath = os.path.join(outputDir,fout)
                seName = '%s' % os.environ['VO_SNOPLUS_SNOLAB_CA_DEFAULT_SE']
                lfcPath = os.path.join(lfcDir,fout)
                args = ['--vo','snoplus.snolab.ca','-d',seName,'-P',sePath,'-l',lfcPath,fout]
                rtc,out,err = ExecuteSimpleCommand(command,args,None,os.getcwd())
                dumpOut['guid'] = out[0]
                dumpOut['se'] = lfcPath
                dumpOut['size'] = os.stat(fout).st_size
                dumpOut['cksum'] = adler32(fout)
        elif gridMode == 'srm':
            srmUrl = 'srm://sehn02.atlas.ualberta.ca/pnfs/atlas.ualberta.ca/data/snoplus'
            #command = 'export X509_USER_PROXY=%s \n'%(myproxy)
            #command += 'srmmkdir %s \n'%(srmDir)
            ##ExecuteSimpleCommand('srmmkdir',[srmDir],None,os.getcwd(),False)
            #ExecuteComplexCommand(os.getcwd(), command, False)
            for fout in outputFiles:
                command = 'export X509_USER_PROXY=%s \n'%(voproxy)
                command += 'lcg-cr'
                seRelPath = '%s/%s' % (outputDir,fout)
                seFullPath = os.path.join(srmUrl,seRelPath)
                lfcDir = os.path.join('lfn:/grid/snoplus.snolab.ca',outputDir)
                lfcPath = os.path.join(lfcDir,fout)
                args = ['--vo','snoplus.snolab.ca','-d',seFullPath,'-l',lfcPath,fout]
                for arg in args:
                    command += ' %s'%arg
                command += '\n'
                rtc,out,err = ExecuteComplexCommand(os.getcwd() , command , False)
                dumpOut['guid'] = out[0]
                dumpOut['se'] = seFullPath
                dumpOut['size'] = os.stat(fout).st_size
                dumpOut['cksum'] = adler32(fout)
            #for fout in outputFiles:
            #    #first copy to the output directory
            #    command = 'export X509_USER_PROXY=%s \n'%(myproxy)
            #    command += 'srmcp'
            #    fileLoc = 'file:///%s'%(os.path.join(os.getcwd(),fout))
            #    srmPath = os.path.join(srmDir,fout)
            #    args = [fileLoc,srmPath]
            #    #ExecuteSimpleCommand(command,args,None,os.getcwd())
            #    for arg in args:
            #        command += ' %s'%arg
            #    ExecuteComplexCommand(os.getcwd() , command)
            #    #then log in the lfc
            #    command = 'export X509_USER_PROXY=%s \n'%(voproxy)
            #    command += 'lcg-rf'
            #    lfcDir = os.path.join('/grid/snoplus.snolab.ca',outputDir)
            #    lfcPath = os.path.join(lfcDir,fout)
            #    args = ['-l',lfcPath,srmPath]
            #    #ExecuteSimpleCommand(command,args,None,os.getcwd())
            #    for arg in args:
            #        command += ' %s'%arg
            #    ExecuteComplexCommand(os.getcwd() , command)
    else:
        #just run a local copy
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        for fout in outputFiles:
            fout = fout.strip()#remove any spaces or other junk
            shutil.copy2(fout,outputDir)
            dumpOut['se'] = '%s:%s'%(socket.gethostname(),os.path.join(outputDir,fout))
            dumpOut['size'] = os.stat(fout).st_size
            dumpOut['cksum'] = adler32(fout)
    return dumpOut

def getData(inputDir,inputFiles,gridMode,voproxy):
    '''Copy the input data across
    #TODO: think about how we can ensure data is at the SE before getting it!
    '''
    inputFiles = inputFiles.strip("[]").split(',')
    if gridMode is not None:
        if gridMode=='lcg':
            lfcDir = os.path.join('lfn:/grid/snoplus.snolab.ca',inputDir)
        else:
            lfcDir = os.path.join('srm://sehn02.atlas.ualberta.ca/pnfs/atlas.ualberta.ca/data/snoplus',inputDir)
        for fin in inputFiles:
            fin = fin.strip()#remove any spaces or other junk
            command = 'export X509_USER_PROXY=%s \n'%(voproxy)
            command += 'lcg-cp'
            lfcPath = '%s/%s' %(lfcDir,fin)
            args = ['--vo','snoplus.snolab.ca',lfcPath,fin]
            for arg in args:
                command += ' %s'%arg
            command += '\n'
            ExecuteComplexCommand(os.getcwd() , command , False)
    else:
        for fin in inputFiles:
            fin = fin.strip()#remove any spaces or other junk
            inputPath = os.path.join(inputDir,fin)
            shutil.copy2(inputPath,fin)

if __name__ == '__main__':
    parser = optparse.OptionParser( usage = "ganga %prog [flags]")
    parser.add_option("-v",dest="ratV",help="RAT version to run")
    parser.add_option("-s",dest="swDir",help="Software install directory")
    parser.add_option("-m",dest="ratMacro",help="RAT macro to use (or script if in shell mode)")
    parser.add_option("-d",dest="outputDir",help="LFC/SURL relative directory if in grid mode, full dir path if not in grid mode")
    parser.add_option("-g",dest="gridMode",default=None,help="Grid mode")
    parser.add_option("-k",action="store_true",dest="shellMode",help="Shell mode: use if passing a shell script to run")
    parser.add_option("-i",dest="inputFiles",help="list of input files, must be in [braces] and comma delimited")
    parser.add_option("-x",dest="inputDir",help="LFC/SURL relative directory if in grid mode, full dir path if not in grid mode")
    parser.add_option("-o",dest="outputFiles",help="list of output files, must be in [braces] and comma delimited")
    parser.add_option("--voproxy",dest="voproxy",default=None,help="VO proxy location, MUST be used with grid srm mode")
    parser.add_option("--myproxy",dest="myproxy",default=None,help="myproxy location, MUST be used with grid srm mode")
    #could also add an option to use a non VO_SNOPLUS_SNOLAB_CA_SW_DIR path
    (options, args) = parser.parse_args()
    if not options.ratV or not options.ratMacro or not options.outputDir or not options.swDir and not options.outputFiles:
        print 'options not all present'
    else:
        if options.inputFiles:
            if not options.inputDir:
                print 'No input directory specified'
                raise Exception
            getData(options.inputDir,options.inputFiles,options.gridMode,options.voproxy)
        if options.gridMode=='srm':
            if not options.voproxy or not options.myproxy:
                print 'Grid %s mode, must define voproxy and myproxy' % options.gridMode
                parser.print_help()
                raise Exception
            elif not os.path.exists(options.voproxy) or not os.path.exists(options.myproxy):
                print 'Grid %s mode, must define valid voproxy and myproxy' % options.gridMode
                parser.print_help()
                raise Exception
        if options.shellMode:
            #assumes not using python.  Append this script to one that
            #sets up the snoing/rat environment
            #ExecuteSimpleCommand( "/bin/bash", [options.ratMacro], None, os.getcwd())
            runScript(options.ratMacro,options.ratV,options.swDir,options.gridMode)
        else:
            runRat(options.ratMacro,options.ratV,options.swDir,options.gridMode)
        dumpOut = copyData(options.outputDir,options.outputFiles,options.gridMode,options.voproxy,options.myproxy)
        returnCard = file('return_card.js','w')
        returnCard.write(json.dumps(dumpOut))
        returnCard.close()
