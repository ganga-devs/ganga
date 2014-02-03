# Tools to ship with every rat job.
# Useful!

import os
import sys
import re
import subprocess

all_env = {}

class JobToolsException(Exception):
    def __init__(self, error):
        Exception.__init__(self, error)


def set_all_env(**kwargs):
    '''Append an environment for use in *all* job_tools calls.
    '''
    global all_env
    for key in kwargs:
        all_env[key] = kwargs[key]


def simple(command,*args):
    '''Simple command.
    '''
    cmdArgs = []
    for arg in args:
        if type(arg)==list:
            cmdArgs+=arg
        else:
            cmdArgs+=[arg]
    rtc,out,err = execute(command,cmdArgs)
    return rtc,out,err


def debug(command,*args):
    '''Simple command, debug mode
    '''
    cmdArgs = []
    for arg in args:
        if type(arg)==list:
            cmdArgs+=arg
        else:
            cmdArgs+=[arg]
    rtc,out,err = execute(command,cmdArgs,debug=True)
    return rtc,out,err

    
def execute(command,args,env=None,cwd=None,verbose=False,debug=False):
    '''Simple command.
    '''
    shellCommand = [ command ] + args
    if verbose:
        print shellCommand
    # Default to current environment
    useEnv = os.environ
    # Append any all_env keys
    for key in all_env:
        useEnv[key] = all_env[key]
    # Append/override any specific keys
    if env is not None:
        for key in env:
            useEnv[key] = env[key]
    if cwd is None:
        cwd = os.getcwd()
    for i,arg in enumerate(args):
        if type(arg) is unicode:
            args[i] = ucToStr(arg)
    if debug:
        print 'cmd',shellCommand
        print 'cwd',cwd
        print 'ls',os.listdir(cwd)
        print 'env',useEnv
    process = subprocess.Popen( args = shellCommand, env = useEnv, cwd = cwd, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = False )
    output = ""
    error = ""
    if verbose:
        for line in iter( process.stdout.readline, "" ):
            sys.stdout.write( '\n' + line[:-1] )
            sys.stdout.flush()
            output += '\n' + line[:-1]
        process.wait()
    else:
        output, error = process.communicate()
    output = output.split('\n')#want lists
    error = error.split('\n')#will always have a '' as last element, unless :
    if output[len(output)-1]=='':
        del output[-1]
    if error[len(error)-1]=='':
        del error[-1]
    return process.returncode,output,error #don't always fail on returncode!=0


def exists(url):
    '''Just checks a file exists
    '''
    command = 'lcg-ls'
    rtc,out,err = simple(command, url)
    if rtc!=0:
        raise JobToolsException('File does not exist %s: %s' % (url, err))


def lcg_ls(*args):
    '''List the contents of an lfn or SURL.
    This could be made a private function - users should use list_dir etc.
    '''
    command='lcg-ls'
    rtc,out,err = simple(command,*args)
    return rtc,out,err


def getsize(url):
    '''Get the size (must be an srm)
    '''
    rtc, out, err = lcg_ls('-l', url)
    if rtc!=0:
        raise JobToolsException('File does not exist %s' % url)
    else:
        try:
            bits = out[0].split()
            size = int(bits[4])
            return size
        except:
            raise JobToolsException('Cannot get size of %s' % url)

        
def getguid(url):
    '''Get the guid of a file
    '''
    rtc,out,err = simple('lcg-lg',url)
    if rtc!=0:
        raise JobToolsException('Cannot get url of %s'%url)
    else:
        guid = out[0]
        return guid


def listreps(url):
    '''Get the locations of a file in the lfc.
    Returns a list.
    '''
    rtc,out,err = simple('lcg-lr',url)
    if rtc!=0:
        raise JobToolsException('lcg-lr error: %s'%url)
    else:
        return out


def checksum(url):
    '''Get the checksum of a file (adler32).
    Must provide an SRM.
    '''
    rtc,out,err = simple('lcg-get-checksum',url)
    if rtc!=0:
        raise JobToolsException('Checksum error: %s'%url)
    else:
        try:
            cks = out[0].split()[0].strip()
            return cks
        except IndexError,e:
            print 'checksum index problem for %s:'%url
            print out
            raise JobToolsException('Checksum error: %s'%url)


def get_se_name(grid_mode):
    if grid_mode == 'srm':
        # Assume this means westgrid
        se_name = "sehn02.atlas.ualberta.ca"
    elif grid_mode == 'lcg':
        se_name = os.environ['VO_SNOPLUS_SNOLAB_CA_DEFAULT_SE']
    else:
        raise JobToolsException('Unknown grid mode %s; cannot get se name' % grid_mode)
    return se_name


def get_se_path(grid_mode):
    se_name = get_se_name(grid_mode)
    rtc, out, err = simple('lcg-info', '--list-se', '--vo', 'snoplus.snolab.ca', '--attrs',
                           'VOInfoPath', '--query', 'SE=%s' % (se_name))
    if rtc!=0:
        raise JobToolsException('Cannot get se-path for %s' % (se_name))
    try:
        se_path = out[-2].split()[-1]
        if se_path[0]=='/':
            se_path = se_path[1:]
        return se_path
    except:
        raise JobToolsException('Cannot get se-path from %s' % (out))
