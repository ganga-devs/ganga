#!/usr/bin/env python
##################################################
#Toolset to assist with GangaSNOplus RATUser and
#RATProd.
##################################################
import os
import optparse
import subprocess
import urllib2
import fnmatch
import tarfile
import shutil
import base64
import re

######################################################################################
# General tools
#
######################################################################################

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

######################################################################################
# Git archiving tools
#
######################################################################################

def DownloadRepo(installPath):
    command = 'git'
    args = ['clone','git@github.com:snoplus/rat.git']
    ExecuteSimpleCommand(command,args,None,installPath)

def UpdateRepo(installPath):
    command = 'git'
    args = ['pull']#what if we have new branches added?
    ExecuteSimpleCommand(command,args,None,installPath)

def ArchiveRepo(installPath,archivePath,snapshot,prefix):
    fileName = '%s/rat.%s.tar.gz' % (archivePath,snapshot)
    tarName  = '%s/rat.%s.tar' % (archivePath,snapshot)
    if os.path.exists(fileName):
        #assume this is the correct snapshot (there's a big old hash code...)
        return fileName
    command = 'git'
    args = ['archive','--format','tar','--prefix',prefix,'-o',tarName,snapshot]
    try:
        ExecuteSimpleCommand(command,args,None,installPath)
    except Exception:
        #try pulling the most recent copy
        UpdateRepo(installPath)
        try:
            ExecuteSimpleCommand(command,args,None,installPath)
        except Exception:
            raise Exception
    command = 'gzip'
    args = [tarName]
    ExecuteSimpleCommand(command,args,None,installPath)
    return fileName

def DownloadSnapshot(version):
    command = 'git'
    args = ['archive','-o','repo.tar','--remote=git@github.com:snoplus/rat',version]
    ExecuteSimpleCommand(command,args,None,os.getcwd())

def MakeRatSnapshot(ratVersion,zipPrefix='archived/',cachePath=os.path.expanduser('~/gaspCache')):
    '''Create a snapshot of RAT from an existing git repo.
    '''
    if not os.path.exists(cachePath):
        os.makedirs(cachePath)
    ratPath = os.path.join(cachePath,'rat')
    if not os.path.exists(ratPath):
        DownloadRepo(cachePath)
    fileName = ArchiveRepo(ratPath,cachePath,ratVersion,zipPrefix)
    return fileName

######################################################################################
# Macro checking tools
#
######################################################################################
def checkCommand(fname,command):
    f = file(fname,'r')
    commandExists = False
    for line in f.readlines():
        if checkCommandLine(command,line):
            commandExists = True
    return commandExists

def checkOption(fname,command):
    f = file(fname,'r')
    optionExists = False
    for line in f.readlines():
        if checkOptionLine(command,line):
            optionExists = True
    return optionExists

def checkCommandLine(command,line):
    '''Checks for a list of commands/options, returns True if all are present.
    Separations by whitespace only.  Only takes lists with 1/2 members currently.
    '''
    pattern = re.compile(r'''\s*(?P<command>\S*)\s*(?P<option>\S*)''')
    search = pattern.search(line)
    parts = ['command','option']
    match=[]
    for i in range(len(command)):
        match.append(0)
    for i,part in enumerate(parts):
        if i>=len(command):
            continue
        if command[i]==search.group(part):
            match[i]=1
    fullMatch=True
    for i in match:
        if i==0:
            fullMatch=False
    return fullMatch

def checkOptionLine(command,line):
    '''Checks for a command string, returns true if an option is present.
    (Will return False if no command is present or if command but no option). 
    '''
    pattern = re.compile(r'''\s*(?P<command>\S*)\s*(?P<option>\S*)''')
    search = pattern.search(line)
    parts = ['command','option']
    hasOption=False
    optionParts = parts[1:]
    commandPart = parts[0]
    if command==search.group(commandPart):
        #the command is present, is there an option?
        for part in optionParts:
            if search.group(part)!='':
                #option is present
                hasOption=True
    return hasOption
