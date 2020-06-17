#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import os.path
import pprint
import tempfile
from GangaCore.Utility.Shell import Shell
from GangaCore.Utility.files import fullpath
from GangaCore.Utility.util import unique
import GangaCore.Utility.logging
import GangaCore.Utility.Config
from GangaCore.Core.exceptions import GangaException
from . import CMTUtils
from . import cmakeUtils
import subprocess
import time
import collections

logger = GangaCore.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def make(self, arguments):

    if not self.newStyleApp is True:
        return CMTUtils.make(arguments)
    else:
        return cmakeUtils.make(arguments)

def get_user_platform(self, env=os.environ):

    if not self.newStyleApp is True:
        return CMTUtils.get_user_platform(env)
    else:
        return cmakeUtils.get_user_platform(env)


def update_project_path(self, user_release_area, env=os.environ):

    if not self.newStyleApp is True:
        return CMTUtils.update_project_path(user_release_area, env)
    else:
        return cmakeUtils.update_project_path(user_release_area, env)

def get_user_dlls(self, appname, version, user_release_area, platform, env):

    if not self.newStyleApp is True:
        return CMTUtils.get_user_dlls(appname, version, user_release_area, platform, env)
    else:
        return cmakeUtils.get_user_dlls(appname, version, user_release_area, platform, env)

def pyFileCollector(dir, file_list, subdir_dict, depth_cut, depth=0, zerodepth_pathlength=0):
    if zerodepth_pathlength == 0:
        zerodepth_pathlength = len(dir) + 1
    sub_pys = []
    for item in os.listdir(dir):
        file_path = os.path.join(dir, item)
        if (file_path.endswith('.py')):
            if os.path.exists(file_path):
                if depth == 0:
                    file_list.append(file_path)
                else:
                    sub_pys.append(file_path)
            else:
                logger.warning(
                    "File %s in %s does not exist. Skipping...", str(item), str(dir))
        elif os.path.isdir(file_path) and not os.path.islink(file_path):
            if depth >= depth_cut:
                continue
            pyFileCollector(
                file_path, file_list, subdir_dict, depth_cut, depth + 1, zerodepth_pathlength)

    if depth != 0:
        subdir_dict[dir[zerodepth_pathlength:]] = sub_pys


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def shellEnv_cmd(cmd, environ=None, cwdir=None):
    pipe = subprocess.Popen(cmd,
                            shell=True,
                            env=environ,
                            cwd=cwdir,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.DEVNULL)
    stdout, stderr = pipe.communicate()
    while pipe.poll() is None:
        time.sleep(0.5)
    return pipe.returncode, stdout, stderr


def shellEnvUpdate_cmd(cmd, environ=os.environ, cwdir=None):
    import tempfile
    import pickle
    f = tempfile.NamedTemporaryFile(mode='w+b')
    fname = f.name
    f.close()

    if not cmd.endswith(';'):
        cmd += ';'
    envdump = 'import os, pickle;'
    envdump += 'f=open(\'%s\',\'w+b\');' % fname
    envdump += 'pickle.dump(os.environ,f);'
    envdump += 'f.close();'
    envdumpcommand = 'python -c \"%s\"' % envdump
    cmd += envdumpcommand

    pipe = subprocess.Popen(cmd,
                            shell=True,
                            env=environ,
                            cwd=cwdir,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.DEVNULL)
    stdout, stderr = pipe.communicate()
    while pipe.poll() is None:
        time.sleep(0.5)

    f = open(fname, 'r+b')
    environ = environ.update(pickle.load(f))
    f.close()
    os.system('rm -f %s' % fname)

    return pipe.returncode, stdout, stderr

    #\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
    # NEW, better version?

CommandOutput = collections.namedtuple(
    'CommandOutput', ['returncode', 'stdout', 'stderr'])


class TimeoutException(Exception):

    def __init__(self, message):
        super(TimeoutException, self).__init__(message)


def run(cmd, env=None, cwd=None, timeout=None):
    proc = subprocess.Popen(cmd,
                            shell=True,
                            env=env,
                            cwd=cwd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.DEVNULL)
    if timeout is not None:
        time_start = time.time()
        while proc.poll() is None:
            if time.time() - time_start >= timeout:
                proc.kill()
                raise TimeoutException("Command '%s' timed out!" % cmd)
            time.sleep(0.5)
    stdout, stderr = proc.communicate()
    return CommandOutput(proc.returncode, stdout, stderr)


def runUpdate(cmd, env=None, cwd=None, timeout=None):
    (fd, filename) = tempfile.mkstemp()
    command_output = run(cmd + '; printenv &> ' + filename, env, cwd, timeout)
    with os.fdopen(fd, 'r') as file:
        env.update(dict([tuple(line.split('=', 1))
                         for line in file.read().splitlines()]))
    return command_output


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def fillPackedSandbox(sandbox_files, destination):
    """Put all sandbox_files into tarball called name and write it into to the destination.
    Arguments:
    'sandbox_files': a list of File or FileBuffer objects.
    'destination': a string representing the destination filename
    Return: a list containing a path to the tarball
    """
    if not sandbox_files:
        return []

    # Generalised version from Ganga/Core/Sandbox/Sandbox.py

    import tarfile
    import stat
    #tf = tarfile.open(destination,"w:gz")

    # "a" = append with no compression
    # creates file if doesn't exist
    # cant append to a compressed tar archive so must compress later
    dir, filename = os.path.split(destination)
    if not os.path.isdir(dir):
        os.makedirs(dir)
    tf = tarfile.open(destination, "a")
    tf.dereference = True  # --not needed in Windows

    for f in sandbox_files:
        try:
            contents = f.getContents()   # is it FileBuffer?

        except AttributeError:         # File
            try:
                fileobj = open(f.name, 'rb')
            except Exception as err:
                raise GangaException("File %s does not exist." % f.name)
            tinfo = tf.gettarinfo(
                f.name, os.path.join(f.subdir, os.path.basename(f.name)))

        else:                          # FileBuffer
            from io import StringIO, BytesIO
            fileobj = BytesIO(contents)

            tinfo = tarfile.TarInfo()
            tinfo.name = os.path.join(f.subdir, os.path.basename(f.name))
            import time
            tinfo.mtime = time.time()
            tinfo.size = len(fileobj.getvalue())

        if f.isExecutable():
            tinfo.mode = tinfo.mode | stat.S_IXUSR
        tf.addfile(tinfo, fileobj)

    tf.close()

    return [destination]


def gzipFile(filename, outputfilename=None, removeOriginal=False):
    """
    This method creates a new compressed version of the inputfile compressed with gzip
    Args:
        filename (str): This is the file which is being compressed
        outputfilename (str): This is the (optional) name of the outputfile object which is created
        removeOriginal (bool): Should the original be destroyed after the compressed version is made?
    """
    import gzip
    if not outputfilename:
        outputfilename = filename + '.gz'
    f_in = open(filename, 'rb')
    f_out = gzip.open(outputfilename, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()
    if removeOriginal:
        os.system('rm -f %s' % filename)

