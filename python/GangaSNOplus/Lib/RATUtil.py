#!/usr/bin/env python
##################################################
# Toolset to assist with GangaSNOplus RATUser and
# RATProd.
##################################################

import os
import optparse
import subprocess
import urllib2
import fnmatch
import tarfile
import shutil
import base64
import getpass
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


def DownloadSnapshot(fork, version, filename):
    """Download a tarball of a given rat version.

    Version may be either the commit hash or the branch name (if latest commit is desired).
    However, a commit hash is preferred as the branch name will mean that newer commits are not 
    grabbed if rerunning at a later date.
    """
    url = "https://github.com/%s/rat/archive/%s.tar.gz" % (fork, version)
    print "URL:", url
    url_request = urllib2.Request(url)
    # Only ever downloading once, so prompt for the username here
    username = raw_input("Username: ") #This might not be the same as the fork...
    password = getpass.getpass("Password: ")
    b64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
    url_request.add_header("Authorization", "Basic %s" % b64string)
    try:
        remote_file = urllib2.urlopen(url_request)
    except urllib2.URLError, e:
        print "Cannot connect to GitHub: ", e
        raise
    download_size = int(remote_file.info().getheaders("Content-Length")[0])
    local_file = open(filename, "wb")    
    local_file.write(remote_file.read())
    local_file.close()
    remote_file.close()


def MakeRatSnapshot(ratFork, ratVersion, versionUpdate, zipPrefix='archived/',
                    cachePath=os.path.expanduser('~/gaspCache')):
    '''Create a snapshot of RAT from an existing git repo.
    '''
    if not os.path.exists(cachePath):
        os.makedirs(cachePath)
    ratPath = os.path.join(cachePath, 'rat')
    # Old method: download rat git repo, make an tarball of the required commit
    # if not os.path.exists(ratPath):
    #     DownloadRepo(cachePath)
    # filename = ArchiveRepo(ratPath,cachePath,ratVersion,zipPrefix)
    # New method: download directly from the url
    filename = os.path.join(cachePath, "rat.%s.%s.tar.gz" % (ratFork, ratVersion))
    if versionUpdate is True or not os.path.exists(filename):
        DownloadSnapshot(ratFork, ratVersion, filename)
    return filename

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
