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

def untarLocal(tarName):
    #Shipped code, just unzip, setup correct env and compile
    installPath = os.getcwd()
    UnTarFile( tarName, installPath, os.getcwd(), 0 )#nothing to strip
    #for some reason, configure is not x by default
    ExecuteSimpleCommand( 'chmod', ['u+x','rat/configure'], None, installPath)

def downloadLocal(token,runVersion):
    installPath = "%s/%s" % (os.getcwd(),"rat")
    downloadRat(token,runVersion,installPath)

def installLocal(baseVersion,runVersion,swDir):
    #should be in the temporary job directory to do this
    installPath = "%s/%s" % (os.getcwd(),"rat")
    createTempEnv(baseVersion,swDir)
    compileRat()
    createFinalEnv(baseVersion,swDir,installPath)
    
def downloadRat(token,runVersion,installPath):
    #download the requested version of RAT, unzip it and
    #move it all to a rat directory
    cachePath = os.getcwd()
    url = "https://api.github.com/repos/snoplus/rat/tarball/%s" % runVersion
    tarName = 'rat.tar.gz'
    DownloadFile(url=url,token=token,fileName=tarName,cachePath=cachePath)
    UnTarFile( tarName, installPath, cachePath, 1 ) # Strip the first directory

def DownloadFile( url, username = None, password = None, token = None, fileName = "" , cachePath=""): # Never hard code a password!
    """ Download a file at url, using the username and password if provided and save into the cachePath. Optional fileName parameter to manually name the file which gets stored in the cachePath"""
    if( fileName == "" ): # OW 07/06/2012
        fileName = url.split('/')[-1]
    tempFile = os.path.join( cachePath, "download-temp" )
    urlRequest = urllib2.Request( url )
    if username != None: # Add simple HTTP authentication
        b64string = base64.encodestring( '%s:%s' % ( username, password ) ).replace( '\n', '' )
        urlRequest.add_header( "Authorization", "Basic %s" % b64string )
    elif token != None: # Add OAuth authentication
        urlRequest.add_header( "Authorization", "token %s" % token )
    try:
        remoteFile = urllib2.urlopen( urlRequest )
    except urllib2.URLError, e: # Server not available
        print e
        print "Server not available"
        raise Exception
    localFile = open( tempFile, 'wb')
    try:
        downloadSize = int( remoteFile.info().getheaders("Content-Length")[0] )
        downloaded = 0 # Amount downloaded
        blockSize = 8192 # Convenient block size
        while True:
            buffer = remoteFile.read( blockSize )
            if not buffer: # Nothing left to download
                break
            downloaded += len( buffer )
            localFile.write( buffer )
        remoteFile.close()
        localFile.close()
    except (KeyboardInterrupt, SystemExit):
        localFile.close()
        remoteFile.close()
        os.remove( tempFile )
        raise
    if downloaded < downloadSize: # Something has gone wrong
        print 'downloaded',downloaded
        print 'downloadSize',downloadSize
        raise Exception
    os.rename( tempFile, os.path.join( cachePath, fileName ) )
    return "Downloaded %i bytes\n" % downloadSize

def UnTarFile( tarFileName, targetPath, cachePath , strip = 0 ):
    """ Untar the file tarFile to targetPath take off the the first strip folders."""
    if strip == 0: # Can untar directly into target
        tarFile = tarfile.open( os.path.join( cachePath, tarFileName ) )
        tarFile.extractall( targetPath )
        tarFile.close()
    else: # Must untar to temp then to target, note target cannot already exist!
        # First untar to a temp directory
        tempDirectory = os.path.join( cachePath, "temp" )
        if os.path.exists( tempDirectory ): # Must be an empty temp directory
            shutil.rmtree( tempDirectory )
        tarFile = tarfile.open( os.path.join( cachePath, tarFileName ) )
        tarFile.extractall( tempDirectory )
        tarFile.close()
        # Now choose how many components to strip
        copyDirectory = tempDirectory
        for iStrip in range( 0, strip ):
            subFolders = os.listdir( copyDirectory )
            if 'pax_global_header' in subFolders:
                subFolders.remove( 'pax_global_header' )
            copyDirectory = os.path.join( copyDirectory, subFolders[0] )
        # Now can copy, first make sure the targetPath does not exist
        if os.path.exists( targetPath ):
            shutil.rmtree( targetPath )
        # Now copy
        shutil.copytree( copyDirectory, targetPath )
        shutil.rmtree( tempDirectory )
    return "Extracted %s\n" % tarFileName

#def UnZipFile( zipFileName, targetPath, strip = 0 ):
#    if strip==0:
#        zipFile = zipfile.ZipFile( os.path.join(os.getcwd(),zipFileName) )
#        zipFile.extractall( targetPath )
#        zipFile.close()
#    else:
#        #TODO - write this!
#        pass

def createTempEnv(baseVersion,swDir):
    envText = createEnv(baseVersion,swDir)
    tempEnvFile = file('%s/installerTemp.sh' % os.getcwd(),'w')
    tempEnvFile.write(envText)
    tempEnvFile.close()

def createFinalEnv(baseVersion,swDir,installPath):
    envText = createEnv(baseVersion,swDir,installPath)
    envFile = file("%s/installerEnv.sh" % os.getcwd(),'w')
    envFile.write(envText)
    envFile.close()

def createEnv(ratV,swDir,ratDir=None):
    #Just copy the base version env file, but remove the source rat/env.sh line
    envFile = '%s/env_rat-%s.sh' % (swDir,ratV)
    if not os.path.exists(envFile):
        print 'Cannot find correct environment: %s'%envFile
        raise Exception
    fin = file(envFile,'r')
    envText=''
    lines = fin.readlines()
    for i,line in enumerate(lines):
        if 'source ' in line:
            if i==len(lines)-1 or i==len(lines)-2:
                pass
            else:
                envText += '%s \n' % line
        else:
            envText += '%s \n' % line
    if ratDir is not None:
        envText += "source %s/env.sh" % ratDir
    return envText

def compileRat():
    #compile Rat
    tempFile = os.path.join(os.getcwd(),'installerTemp.sh')
    ratDir   = os.path.join(os.getcwd(),'rat')
    commandText = """#!/bin/bash\nsource %s\ncd %s\n./configure\nsource env.sh\nscons""" % ( tempFile, ratDir )
    ExecuteComplexCommand( os.getcwd() , commandText )
    #running locally, seems rat is not executable by default!
    ExecuteSimpleCommand('chmod',['u+x','rat/bin/rat'],None,os.getcwd())

def ExecuteComplexCommand( installPath, command , exitIfFail=True):
    """ Execute a multiple line bash command, writes to a temp bash file then executes it."""
    print 'installPath:',installPath
    fileName = os.path.join( installPath, "temp.sh" )
    commandFile = open( fileName, "w" )
    commandFile.write( command )
    commandFile.close()
    rtc,out,err = ExecuteSimpleCommand( "/bin/bash", [fileName], None, installPath, exitIfFail)
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

def runRat( ratMacro , baseVersion , runVersion , swDir , inputFile , outputFile , nEvents):
    '''Run the RAT macro
    '''
    command = ''
    if runVersion!=None:
        command += 'source installerEnv.sh \n'
    else:
        envFile = '%s/env_rat-%s.sh' % (swDir,baseVersion)
        if not os.path.exists(envFile):
            print 'Cannot find %s' % envFile
            raise Exception
        command += 'source %s \n' % (envFile)
    ratCmd = 'rat '
    ratCmd += '-l rat.log '
    if inputFile:
        ratCmd += '-i %s ' % inputFile
    if outputFile:
        ratCmd += '-o %s ' % outputFile
    if nEvents:
        ratCmd += '-N %s ' % nEvents
    ratCmd += ' %s ' % ratMacro
    command += '%s \n' % ratCmd
    ExecuteComplexCommand(os.getcwd() , command)
    print os.listdir(os.getcwd())

def copyData(outputDir,gridMode,voproxy,myproxy):
    '''Copy all output data (i.e. any .root output files)
    gridMode true: use lcg-cr to copy and register file
             false: use cp (the job runs in a temp dir, any files left at end are deleted eventually)
    '''
    rootFiles = fnmatch.filter(os.listdir(os.getcwd()),'*.root')
    print 'copData: grid',gridMode,'files:',rootFiles
    dumpOut = {}
    if gridMode is not None:
        if gridMode=='lcg':
            lfcDir = os.path.join('lfn:/grid/snoplus.snolab.ca',outputDir)
            ExecuteSimpleCommand('lfc-mkdir',[lfcDir.lstrip('lfn:')],None,os.getcwd(),False)
            for rf in rootFiles:
                command = 'lcg-cr'
                sePath = '%s/%s' % (outputDir,rf)
                seName = '%s' % os.environ['VO_SNOPLUS_SNOLAB_CA_DEFAULT_SE']
                lfcPath= '%s/%s' % (lfcDir,rf)
                args = ['--vo','snoplus.snolab.ca','-d',seName,'-P',sePath,'-l',lfcPath,rf]
                rtc,out,err = ExecuteSimpleCommand(command,args,None,os.getcwd())
                dumpOut['guid'] = out[0]
                dumpOut['se'] = lfcPath
                dumpOut['size'] = os.stat(rf).st_size
                dumpOut['cksum'] = adler32(rf)
        elif gridMode=='srm':
            srmUrl = 'srm://sehn02.atlas.ualberta.ca/pnfs/atlas.ualberta.ca/data/snoplus'
            #No need to make the lfn or srm directories on westgrid!
            #command = 'export X509_USER_PROXY=%s \n'%(myproxy)
            #command += 'srmmkdir %s \n'%(srmDir)
            #ExecuteComplexCommand(os.getcwd() , command , False)
            for rf in rootFiles:
                #only use one proxy now, could set in the wrapper script and we wouldn't need to run a shell script!
                command = 'export X509_USER_PROXY=%s \n'%(voproxy)
                command += 'lcg-cr'
                seRelPath = '%s/%s' % (outputDir,rf)
                seFullPath = os.path.join(srmUrl,seRelPath)
                lfcDir = os.path.join('lfn:/grid/snoplus.snolab.ca',outputDir)
                lfcPath = os.path.join(lfcDir,rf)
                args = ['--vo','snoplus.snolab.ca','-d',seFullPath,'-l',lfcPath,rf]
                for arg in args:
                    command += ' %s'%arg
                command += '\n'
                rtc,out,err = ExecuteComplexCommand(os.getcwd() , command , False)
                dumpOut['guid'] = out[0]
                dumpOut['se'] = seFullPath
                dumpOut['size'] = os.stat(rf).st_size
                dumpOut['cksum'] = adler32(rf)
                #first copy to the output directory
                #command = 'export X509_USER_PROXY=%s \n'%(myproxy)
                #command += 'srmcp'
                #fileLoc = 'file:///%s'%(os.path.join(os.getcwd(),rf))
                #srmPath = os.path.join(srmDir,rf)
                #args = [fileLoc,srmPath]
                #for arg in args:
                #    command += ' %s'%arg
                #ExecuteComplexCommand(os.getcwd() , command)
                #then log in the lfc
                #command = 'export X509_USER_PROXY=%s \n'%(voproxy)
                #command += 'lcg-rf'
                #lfcDir = os.path.join('/grid/snoplus.snolab.ca',outputDir)
                #lfcPath = os.path.join(lfcDir,rf)
                #args = ['-l',lfcPath,srmPath]
                #for arg in args:
                #    command += ' %s'%(arg)
                #ExecuteComplexCommand(os.getcwd() , command)
        else:
            print 'Error, bad copy mode:',gridMode
            raise Exception
    else:
        #just run a local copy
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        for rf in rootFiles:
            shutil.copy2(rf,outputDir)
            dumpOut['se'] = '%s:%s'%(socket.gethostname(),os.path.join(outputDir,rf))
            dumpOut['size'] = os.stat(rf).st_size
            dumpOut['cksum'] = adler32(rf)
    return dumpOut

if __name__ == '__main__':
    print 'ratRunner...??'
    parser = optparse.OptionParser( usage = "ganga %prog [flags]")
    parser.add_option("-t",dest="token",help="Token to use, if in token mode")
    parser.add_option("-b",dest="baseV",help="RAT base version")
    parser.add_option("-v",dest="runV",help="RAT version to run")
    parser.add_option("-s",dest="swDir",help="Software install directory")#,default="$VO_SNOPLUS_SNOLAB_CA_SW_DIR/snoing-install")
    parser.add_option("-m",dest="ratMacro",help="RAT macro to use")
    parser.add_option("-d",dest="outputDir",help="LFC/SURL relative directory if in grid mode, full dir path if not in grid mode")
    parser.add_option("-g",dest="gridMode",default=None,help="Grid mode (allowed modes are srm and lcg)")
    parser.add_option("-f",dest="zipFileName",help="Zip filename for output file")
    parser.add_option("-i",dest="inputFile",default=None,help="Specify input file")
    parser.add_option("-o",dest="outputFile",default=None,help="Specify output file")
    parser.add_option("-n",dest="nEvents",default=None,help="Number of events (must not be in macro)")
    parser.add_option("--voproxy",dest="voproxy",default=None,help="VO proxy location, MUST be used with grid srm mode")
    parser.add_option("--myproxy",dest="myproxy",default=None,help="myproxy location, MUST be used with grid srm mode")
    #could also add an option to use a non VO_SNOPLUS_SNOLAB_CA_SW_DIR path
    (options, args) = parser.parse_args()
    if not options.baseV:
        print ' need base version'
    if not options.runV:
        print 'RUNNING WITH A FIXED RELEASE'
    if not options.ratMacro:
        print ' need macro'
    if not options.outputDir:
        print ' need output directory'
    if not options.swDir:
        print ' need software directory'
    if options.gridMode=='srm':
        if not options.voproxy or not options.myproxy:
            print 'Grid %s mode, must define voproxy and myproxy' % options.gridMode
            parser.print_help()
            raise Exception
        elif not os.path.exists(options.voproxy) or not os.path.exists(options.myproxy):
            print 'Grid %s mode, must define valid voproxy and myproxy' % options.gridMode
            parser.print_help()
            raise Exception
    if not options.baseV or not options.ratMacro or not options.outputDir or not options.swDir:
        print 'options not all present, cannot run'
        parser.print_help()
        raise Exception
    elif options.runV and not options.token and not options.zipFileName:
        print 'must choose one of token/filename to run with a specific hash version'
    elif options.token and options.zipFileName:
        print 'must choose only one of token/filename'
    else:
        if options.runV:
            if options.token:
                downloadLocal(options.token,options.runV)
            else:
                untarLocal(options.zipFileName)
            #all args have to be strings anyway (from Ganga) - force base version into a string
            installLocal(str(options.baseV),options.runV,options.swDir)
        runRat(options.ratMacro,options.baseV,options.runV,options.swDir,options.inputFile,options.outputFile,options.nEvents)
        dumpOut = copyData(options.outputDir,options.gridMode,options.voproxy,options.myproxy)
        returnCard = file('return_card.js','w')
        returnCard.write(json.dumps(dumpOut))
        returnCard.close()
