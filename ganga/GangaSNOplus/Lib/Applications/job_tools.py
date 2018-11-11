# Tools to ship with every rat job.
# Useful!

import os
import sys
import re
import subprocess


###########################################
# Exceptions
###########################################
class JobToolsException(Exception):
    def __init__(self, error):
        Exception.__init__(self, error)


class CommandException(JobToolsException):
    def __init__(self, command, out, err):
        '''Pass in the command failed and outputs/errors
        '''
        error = '\nCommand: %s' % command
        error += '\nOutput: %s' % out
        error += '\nError: %s' % err
        JobToolsException.__init__(self, error)


###########################################
# Other classes
###########################################
class JobHelper(object):
    '''A singleton class to persist information relating to job setup.

    Useful for jobs where multiple functions need to be called that all
    need the same environment!
    '''
    
    _instance = None

    class SingletonHelper:
        def __call__(self, *args, **kw):
            if JobHelper._instance is None:
                object = JobHelper()
                JobHelper._instance = object
            return JobHelper._instance

    get_instance = SingletonHelper()

    def __init__(self):
        '''Nothing done in the constructor'''
        self._envs = []
        self._vars = {}

    def add_environment(self, path):
        '''Append an environment file that should be used.

        These must be added in the correct order.'''
        if not os.path.exists(path):
            raise JobToolsException("JobHelper: no such file %s" % path)
        self._envs.append(path)

    def set_variables(self, **kwargs):
        '''Add an environment variable; these are set after before environment files are sourced.
        '''
        for key in kwargs:
            self._vars[key] = kwargs[key]

    def get_environment_script(self):
        '''Return the environment as required for jobs'''
        command = ""
        for env in self._envs:
            command += "source %s\n" % env
        return command

    def get_environment_variables(self):
        '''Return the environment variables as required for jobs
        '''
        return self._vars


###########################################
# Execution commands
###########################################
def execute_ratenv(command, args, cwd=None, exit_if_fail=True):
    '''Sources appropriate environment information prior to call.
    
    Provide string command and list of arguments as with "execute".
    This is the only command that raises an exception if if fails by default.
    '''
    job_helper = JobHelper.get_instance()
    script = job_helper.get_environment_script()
    script += command
    for arg in args:
        script += ' %s' % arg
    script += '\n'
    return execute_complex(script, cwd=cwd, exit_if_fail=exit_if_fail)


def execute_complex(script, cwd=None, exit_if_fail=False, login_script=False):
    '''Write a temporary script, run, remove.
    '''
    if cwd is None:
        cwd = os.getcwd()
    filename = os.path.join(cwd, "temp.sh")
    temp_file = file(filename, 'w')
    temp_file.write(script)
    temp_file.close()
    # Don't pass the exit_if_fail on to execute, rather print out info here
    # Otherwise just get confusing errors regarding temp.sh
    args = [filename]
    if login_script:
        args = ['-l', filename]
    rtc, out, err = execute('/bin/bash', args)
    if exit_if_fail is True and rtc!=0:
        raise CommandException(script, out, err)
    return rtc, out, err


def execute(command, args, env=None, cwd=None, exit_if_fail=False):
    '''Simple command.
    '''
    shell_command = [command] + args
    # Default to current environment, then apply overrides from job_helper
    # then apply overrides from function arguments
    use_env = os.environ
    job_helper = JobHelper.get_instance()
    for env in job_helper.get_environment_variables():
        use_env[key] = env[key]
    if env is not None:
        for key in env:
            use_env[key] = env[key]
    if cwd is None:
        cwd = os.getcwd()
    for i,arg in enumerate(args):
        if type(arg) is unicode:
            args[i] = ucToStr(arg) # urgh
    process = subprocess.Popen(args=shell_command, env=use_env, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    output, error = process.communicate()
    if process.returncode != 0:
        sys.stderr.write("Process failed: %s \n" % shell_command)
        sys.stderr.write("Output: %s \n" % output)
        sys.stderr.write("Error: %s \n" % error)
        if exit_if_fail is True:
            raise CommandException(shell_command, output, error)
    output = output.split('\n') # want lists
    error = error.split('\n') # will always have a '' as last element, unless :
    if output[len(output)-1]=='':
        del output[-1]
    if error[len(error)-1]=='':
        del error[-1]
    return process.returncode, output, error # don't always fail on returncode!=0


###########################################
# Data transfer and checking tools
###########################################
def copy_data(file_list, output_dir, grid_mode):
    '''Transfer files from file list to the given directory.
    
    TODO: exit if upload fails, but first delete any files that have already been uploaded.
    '''
    dump_out = {}
    if grid_mode is not None:

        if grid_mode=='lcg':            
            lfc_dir = os.path.join('lfn:/grid/snoplus.snolab.ca', output_dir)
            execute('lfc-mkdir', [lfc_dir.lstrip('lfn:')], exit_if_fail=False)
            se_name = os.environ['VO_SNOPLUS_SNOLAB_CA_DEFAULT_SE']
            args = ['--list-se', '--vo', 'snoplus.snolab.ca', '--attrs', 'VOInfoPath', '--query', 'SE=%s' % (se_name)]
            rtc, out, err = execute('lcg-info', args)
            srm_path = "INCOMPLETE" # in case we aren't able to get the path
            if rtc==0:
                # remove the leading / so that os.path.join works
                srm_path = out[-2].split()[-1].lstrip('/')
            for filename in file_list:
                dump_out[filename] = {}
                command = 'lcg-cr'
                se_path = os.path.join(output_dir, filename)
                lfc_path= os.path.join(lfc_dir, filename)
                args = ['--vo', 'snoplus.snolab.ca', '--checksum', '-d', se_name, '-P', se_path, '-l', lfc_path, filename]
                rtc, out, err = execute(command, args)
                dump_out[filename]['guid'] = out[0]
                dump_out[filename]['se'] = os.path.join('srm://%s' % se_name, srm_path, se_path)
                dump_out[filename]['size'] = os.stat(filename).st_size
                dump_out[filename]['cksum'] = adler32(filename)
                dump_out[filename]['name'] = se_name
                dump_out[filename]['lfc'] = lfc_path

        elif grid_mode=='srm':
            se_name = 'sehn02.atlas.ualberta.ca'
            surl = 'srm://sehn02.atlas.ualberta.ca/pnfs/atlas.ualberta.ca/data/snoplus'
            for filename in file_list:
                dump_out[filename] = {}
                command = 'lcg-cr'
                se_rel_path = os.path.join(output_dir, filename)
                se_full_path = os.path.join(surl, se_rel_path)
                lfc_dir = os.path.join('lfn:/grid/snoplus.snolab.ca', output_dir)
                lfc_path = os.path.join(lfc_dir, filename)
                args = ['--vo', 'snoplus.snolab.ca', '--checksum', '-d', se_full_path, '-l', lfc_path, filename]
                rtc,out,err = execute(command, args)
                dump_out[filename]['guid'] = out[0]
                dump_out[filename]['se'] = se_full_path
                dump_out[filename]['size'] = os.stat(filename).st_size
                dump_out[filename]['cksum'] = adler32(filename)
                dump_out[filename]['name'] = se_name
                dump_out[filename]['lfc'] = lfc_path
        else:
            raise Exception('Error, bad copy mode: %s' % grid_mode)
    else:
        import shutil
        import socket
        # Just run a local copy
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        for filename in file_list:
            shutil.copy2(filename, output_dir)
            dump_out[filename] = {}
            dump_out[filename]['se'] = '%s:%s' % (socket.getfqdn(), os.path.join(output_dir, filename))
            dump_out[filename]['size'] = os.stat(filename).st_size
            dump_out[filename]['cksum'] = adler32(filename)
            dump_out[filename]['name'] = socket.getfqdn()
    return dump_out


def get_data(file_list, input_dir, grid_mode):
    '''Get files from a list and transfer to a local directory.

    TODO: ensure that this tries to access to closest local copy.
    '''
    if grid_mode is not None:
        # Both SRM and LCG mode should be fine using the LFC
        # TODO: ensure that the closest local copy is retrieved!
        lfc_dir = os.path.join('lfn:/grid/snoplus.snolab.ca', input_dir)
        for fin in file_list:
            fin = fin.strip() # Remove any spaces or other junk
            lfc_path = os.path.join(lfc_dir, fin)
            args = ['--vo','snoplus.snolab.ca', lfc_path, fin]
            execute('lcg-cp', args, exit_if_true=True)
    else:
        import shutil
        for fin in file_list:
            fin = fin.strip()#remove any spaces or other junk
            input_path = os.path.join(input_dir, fin)
            shutil.copy2(input_path, fin)


###########################################
# File utilities
###########################################
def adler32(filename):
    '''Calculate and return Alder32 checksum of file.
    '''
    import zlib
    block = 32 * 1024 * 1024
    val = 1
    f = open(filename, 'rb')
    while True:
        line = f.read(block)
        if len(line) == 0:
            break
        val = zlib.adler32(line, val)
        if val < 0:
            val += 2**32
    f.close()
    return hex(val)[2:10].zfill(8).lower()


def download_file(url, token=None, filename=None , cache_path=None):
    '''Download a file from a URL.  No user/password auth is allowed (security).  Tokens only.
    '''
    import urllib2
    if filename is None:
        filename = url.split('/')[-1]
    temp_file = os.path.join(cache_path, "download-temp")
    url_request = urllib2.Request(url)
    if token is not None:
        # Add OAuth authentication
        url_request.add_header("Authorization", "token %s" % token)
    remote_file = urllib2.urlopen(url_request) # Will raise exception if fails
    local_file = open(temp_file, 'wb')
    try:
        download_size = int(remote_file.info().getheaders("Content-Length")[0])
        downloaded = 0 # Amount downloaded
        block = 8192 # Convenient block size
        while True:
            buffer = remote_file.read(block)
            if not buffer: # Nothing left to download
                break
            downloaded += len(buffer)
            local_file.write(buffer)
        remote_file.close()
        local_file.close()
    except (KeyboardInterrupt, SystemExit):
        local_file.close()
        remote_file.close()
        os.remove(temp_file)
        raise
    if downloaded < download_size: # Something has gone wrong
        raise Exception("Download size error: %s <  %s" % (downloaded, download_size))
    os.rename(temp_file, os.path.join(cache_path, filename))


def untar_file(tarname, target_path, target_dir, cache_path, strip=0):
    '''Untar the file tarname to target_path, removing the first strip folders.
    '''
    import tarfile
    import shutil
    tarred_file = tarfile.open(os.path.join(cache_path, tarname))
    if strip==0:
        tarred_file.extractall(target_path)
        # Mote the file to the appropraite name and close the tar
        basedir = os.path.commonprefix(tarred_file.getnames())
        if basedir=='.':
            raise Exception("Cannot find a common base name")
        shutil.move(os.path.join(cache_path, basedir), os.path.join(cache_path, target_dir))
        tarred_file.close()
    else:
        # Move to temp dir, then target dir
        temp_dir = os.path.join(cache_path, 'temp_tar_dir')
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        tarred_file.extractall(temp_dir)
        tarred_file.close()
        # Now strip the dirs and move to the final location
        copy_dir = temp_dir
        for i_strip in range(0, strip):
            sub_folders = os.listdir(copy_dir)
            if 'pax_global_header' in sub_folders:
                sub_folders.remove( 'pax_global_header' )
            copy_dir = os.path.join(copy_dir, sub_folders)
        if os.path.exists(target_path):
            shutil.rmtree(target_path)
        shutil.copytree(copy_dir, target_path)
        shutil.rmtree(temp_dir)

###########################################
# Grid commands
###########################################
def exists(url):
    '''Just checks a file exists
    '''
    command = 'lcg-ls'
    rtc, out, err = execute(command, [url])
    if rtc!=0:
        raise JobToolsException('File does not exist %s: %s' % (url, err))


def lcg_ls(*args):
    '''List the contents of an lfn or SURL.
    This could be made a private function - users should use list_dir etc.
    '''
    command='lcg-ls'
    passargs = []
    for arg in args:
        passargs.append(arg)
    rtc,out,err = execute(command, passargs)
    return rtc, out, err


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
    rtc,out,err = execute('lcg-lg', [url])
    if rtc!=0:
        raise JobToolsException('Cannot get url of %s'%url)
    else:
        guid = out[0]
        return guid


def listreps(url):
    '''Get the locations of a file in the lfc.
    Returns a list.
    '''
    rtc,out,err = execute('lcg-lr', [url])
    if rtc!=0:
        raise JobToolsException('lcg-lr error: %s'%url)
    else:
        return out


def checksum(url):
    '''Get the checksum of a file (adler32).
    Must provide an SRM.
    '''
    rtc,out,err = execute('lcg-get-checksum', [url])
    if rtc!=0:
        raise JobToolsException('Checksum error: %s'%url)
    else:
        try:
            cks = out[0].split()[0].strip()
            return cks
        except IndexError as e:
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
    rtc, out, err = execute('lcg-info', ['--list-se', '--vo', 'snoplus.snolab.ca', '--attrs',
                                         'VOInfoPath', '--query', 'SE=%s' % (se_name)])
    if rtc!=0:
        raise JobToolsException('Cannot get se-path for %s' % (se_name))
    try:
        se_path = out[-2].split()[-1]
        if se_path[0]=='/':
            se_path = se_path[1:]
        return se_path
    except:
        raise JobToolsException('Cannot get se-path from %s' % (out))

###########################################
# Macro checks
###########################################
def check_processor(filename, processor):
    '''Check whether a processor is implemented.
    '''
    for line in open(filename, 'r').readlines():
        proc = '/proc/%s' % processor
        proc_last = '/proc_last/%s' % processor
        if proc in line or proc_last in line:
            if line.strip()[0]!='#':
                return True
    return False
