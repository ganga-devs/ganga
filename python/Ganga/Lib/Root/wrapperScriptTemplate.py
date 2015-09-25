#!/usr/bin/env python
from __future__ import print_function
'''Script to run root with cint or python.'''
def downloadAndUnTar(fileName, url):
    '''Downloads and untars a file with tar xfz'''
    from shutil import copyfileobj
    from urllib2 import urlopen

    urlFileIn  = urlopen(url)
    urlFileOut = file(fileName,'w')
    copyfileobj(urlFileIn, urlFileOut)
    urlFileOut.close

    status = 0
    cmd = 'tar xzf %s' % fileName

    from commands import getstatusoutput, getoutput

    #to check whether the folder name  is 'root' or 'ROOT'
    folderName = getoutput('tar --list --file ROOT*.tar.gz').split('/')[0]

    try:#do this in try as module is only unix
        #commmand approach removes ugly tar error
        (status,output) = getstatusoutput(cmd)
    except ImportError:
        import os
        status = os.system(cmd)

    return status, folderName

def setEnvironment(key, value, update=False):
    '''Sets an environment variable. If update=True, it preends it to
       the current value with os.pathsep as the seperator.'''
    from os import environ, pathsep
    if update and key in environ:
        value += (pathsep + environ[key])#prepend
    environ[key] = value

def findPythonVersion(arch,rootsys):
    '''Digs around in rootsys for config files and then greps
       the version of python used'''
    import os

    def lookInFile(config):
        '''Looks in the specified file for the build config
           and picks out the python version'''
        version = None
        if os.path.exists(config):
            configFile = file(config)
            for line in configFile:#loop through the file looking for #define
                if line.startswith('#define R__CONFIGUREOPTION'):
                    for arg in line.split(' '):#look at value of #define
                        if arg.startswith('PYTHONDIR'):
                            arglist = arg.split('/')
                            if arglist[-1] == arch:
                                version = arglist[-2]
        return version

    def useRootConfig(rootsys):
        '''Use the new root-config features to find the python version'''
        version = None
        root_config = os.path.join(rootsys,'bin','root-config')
        if os.path.exists(root_config):
            import subprocess

            args = [root_config,'--python-version']

            run = subprocess.Popen(' '.join(args), shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = [ e.splitlines() for e in run.communicate() ]
            code = run.returncode
            if code == 0 and out and not err and len(out) == 1:
                split = out[0].split('.')
                if len(out) != len(split):
                    version = '.'.join(split)
        return version

    version = None
    for f in ['config.h','RConfigure.h']:
        version = lookInFile(os.path.join(rootsys,'include',f))
        if version is not None:
            break
    if version is None:
        version = useRootConfig(rootsys)
    return version

def greaterThanVersion(version_string, version_tuple):
    '''Checks whether a version string is greater than a specific version'''
    result = False
    version_split = version_string.split('.')
    if len(version_split) == 3:
        try:
            major = int(version_split[0])
            minor = int(version_split[1])
            if major >= version_tuple[0] and minor > version_tuple[1]:
                result = True
        except:
            pass
    return result

def findArch(version):
    '''Method stub. In the future we might look at the
       environment to determin the arch we are running on.'''

    #SPI achitectures changed in Root > 5.16
    if greaterThanVersion(version, (5,16) ):
        return 'slc4_ia32_gcc34'
    return 'slc3_ia32_gcc323'

def findURL(version, arch):

    if greaterThanVersion(version, (5,16) ):
        fname = 'ROOT_%s__LCG_%s.tar.gz' % (version,arch)
    else:
        fname = 'root_%s__LCG_%s.tar.gz' % (version,arch)

    return fname



# Main
if __name__ == '__main__':

    from os import curdir, system, environ, pathsep, sep
    from os.path import join
    import sys

    commandline = ###COMMANDLINE###
    scriptPath = '###SCRIPTPATH###'
    usepython = ###USEPYTHON###

    version = '###ROOTVERSION###'
    arch = findArch(version)
    fname = findURL(version, arch)

    spiURL = 'http://service-spi.web.cern.ch/service-spi/external/distribution/'
    url = spiURL + fname

    print('Downloading ROOT version %s from %s.' % (version,url))
    (status, folderName) = downloadAndUnTar(fname,url)
    sys.stdout.flush()
    sys.stderr.flush()

    #see HowtoPyroot in the root docs
    import os
    pwd = os.environ['PWD']
    rootsys=join(pwd,folderName,version,arch,'root')
    setEnvironment('LD_LIBRARY_PATH',curdir,True)
    setEnvironment('LD_LIBRARY_PATH',join(rootsys,'lib'),True)
    setEnvironment('ROOTSYS',rootsys)
    setEnvironment('PATH',join(rootsys,'bin'),True)

    if usepython:

        pythonVersion = findPythonVersion(arch,rootsys)
        if not pythonVersion:
            print('Failed to find the correct version of python to use. Exiting', file=sys.stderr)
            sys.exit(-1)

        tarFileName = 'Python_%s__LCG_%s.tar.gz' % (pythonVersion, arch)
        url = spiURL + tarFileName

        print('Downloading Python version %s from %s.' % (pythonVersion,url))
        downloadAndUnTar(tarFileName,url)

        pythonDir = join('.','Python',pythonVersion,arch)
        pythonCmd = join(pythonDir,'bin','python')
        commandline = commandline % {'PYTHONCMD':pythonCmd}

        setEnvironment('LD_LIBRARY_PATH',join(pythonDir,'lib'),True)
        setEnvironment('PYTHONDIR',pythonDir)
        setEnvironment('PYTHONPATH',join(rootsys,'lib'),True)

    #exec the script
    print('Executing ',commandline)
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(system(commandline)>>8)


