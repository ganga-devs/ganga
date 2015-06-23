import os
import sys
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger(modulename=True)

from WNSandbox import OUTPUT_TARBALL_NAME, PYTHON_DIR
from Ganga.Core import GangaException


class SandboxError(GangaException):

    def __init__(self, message):
        GangaException.__init__(self, message)
        self.message = message

    def __str__(self):
        return "SandboxError: %s " % (self.message)


# FIXME: os.system error handling missing in this module!

def getDefaultModules():
    """ Return list of ganga modules which are needed for WNSandbox. """
    import Ganga.Utility.files
    return [Ganga, Ganga.Utility, Ganga.Utility.files]


def getGangaModulesAsSandboxFiles(modules):
    """ This returns a list of sandbox files corresponding to specified Ganga modules.
    Ganga modules are placed in a well-known location in the sandbox.
    """
    import inspect
    import sys
    from Ganga.Utility.files import remove_prefix
    from Ganga.GPIDev.Lib.File import File

    files = []
    for m in modules:
        fullpath = os.path.realpath(inspect.getsourcefile(m))
        dir, fn = os.path.split(remove_prefix(fullpath, sys.path))
        if os.path.join(dir, fn) == fullpath:
            raise Exception('Cannot find the prefix for %s' % fullpath)
        files.append(File(fullpath, subdir=os.path.join(PYTHON_DIR, dir)))
    return files


def createPackedInputSandbox(sandbox_files, inws, name):
    """Put all sandbox_files into tarball called name and write it into to the input workspace.
       This function is called by Ganga client at the submission time.
       Arguments:
                'sandbox_files': a list of File or FileBuffer objects.
                'inws': a InputFileWorkspace object
       Return: a list containing a path to the tarball
       """

#    from Ganga.Core import FileWorkspace
#    from Ganga.GPIDev.Lib.File import File

#    tgzfile = os.path.join(tmpdir,name)

    tgzfile = inws.getPath(name)

    import tarfile
    import stat

#
# Curent release with os module
#

#   wsdir = os.path.join(tmpdir,"ws")
#   ws = FileWorkspace.FileWorkspace(wsdir)
#   ws.create()
#   for f in sandbox_files:
#       ws.writefile(f)

    # if os.system("tar -C %s -czf %s ."%(wsdir,tgzfile)) !=0:
    #       print "ERROR:: can't create tarball file with InputSandbox"

#
# Future release with tarball module
    with open(tgzfile, 'w') as this_tarfile:
        tf = tarfile.open(name=tgzfile, fileobj=this_tarfile, mode="w:gz")
        tf.dereference = True  # --not needed in Windows

        for f in sandbox_files:
            fileobj = None
            try:
                contents = f.getContents()   # is it FileBuffer?
                # print "Getting FileBuffer Contents"

            except AttributeError:         # File
                # print "Getting File %s" % f.name
                # tf.add(f.name,os.path.join(f.subdir,os.path.basename(f.name)))
                try:
                    fileobj = open(f.name)
                except:
                    raise SandboxError("File %s does not exist." % f.name)
                tinfo = tf.gettarinfo(
                    f.name, os.path.join(f.subdir, os.path.basename(f.name)))

            else:                          # FileBuffer
                from StringIO import StringIO
                fileobj = StringIO(contents)

                tinfo = tarfile.TarInfo()
                # FIX for Ganga/test/Internals/FileBuffer_Sandbox
                # Don't keep the './' on files as looking for an exact filename
                # afterwards won't work
                if f.subdir == os.curdir:
                    tinfo.name = os.path.basename(f.name)
                else:
                    tinfo.name = os.path.join(f.subdir, os.path.basename(f.name))
                import time
                tinfo.mtime = time.time()
                tinfo.size = fileobj.len

            if f.isExecutable():
                tinfo.mode = tinfo.mode | stat.S_IXUSR
            tf.addfile(tinfo, fileobj)
            fileobj.close()
        tf.close()

    return [tgzfile]


def createInputSandbox(sandbox_files, inws):
    """Put all sandbox_files into the input workspace.
       This function is called by Ganga client at the submission time.
       Arguments:
                'sandbox_files': a list of File or FileBuffer objects.
                'inws': a InputFileWorkspace object
       Return: a list of paths to sanbdox files in the input workspace
    """

    #    from Ganga.Core import FileWorkspace

    return [inws.writefile(f, f.isExecutable()) for f in sandbox_files]


def getPackedOutputSandbox(src_dir, dest_dir):
    """Unpack output files from tarball in source directory and
       write them to destination directory
       This function is called by Ganga client at the completing of job.
       Complementary to createPacedOutput()
       Arguments:
                'src_dir': source directory with tarball
                'dest_dir': desti nation directory for output files
    """

    tgzfile = os.path.join(src_dir, OUTPUT_TARBALL_NAME)
    if os.access(tgzfile, os.F_OK):

        import tarfile

        try:
            tf = tarfile.open(tgzfile, "r:gz")
        except tarfile.ReadError:
            logger.warning('Sandbox is empty or unreadable')
            return
        else:
            [tf.extract(tarinfo, dest_dir) for tarinfo in tf]
            tf.close()


#####################################################
