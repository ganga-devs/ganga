from __future__ import absolute_import
import os
import sys
import mimetypes
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger(modulename=True)

from .WNSandbox import OUTPUT_TARBALL_NAME, PYTHON_DIR
from Ganga.Core import GangaException, GangaIOError


class SandboxError(GangaException):

    def __init__(self, message=''):
        GangaException.__init__(self, message)
        self.message = message

    def __str__(self):
        return "SandboxError: %s " % (self.message)


# FIXME: os.system error handling missing in this module!

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

    logger.debug("Creating packed Sandbox with %s many sandbox files." % len(sandbox_files))

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

    if mimetypes.guess_type(tgzfile)[1] in ['gzip']:
        file_format = 'gz'
    elif mimetypes.guess_type(tgzfile)[1] in ['bzip2']:
        file_format = 'bz2'
    else:
        file_format = ''

    with open(tgzfile, 'w:%s' % file_format) as this_tarfile:
        tf = tarfile.open(name=tgzfile, fileobj=this_tarfile, mode="w:gz")
        tf.dereference = True  # --not needed in Windows

        from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
        from Ganga.GPIDev.Lib.File.File import File
        from Ganga.GPIDev.Base.Proxy import isType

        for f in sandbox_files:
            fileobj = None
            if isType(f, FileBuffer):
                contents = f.getContents()   # is it FileBuffer?
                # print "Getting FileBuffer Contents"

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

            else:
                #   except AttributeError as err:         # File
                # print "Getting File %s" % f.name
                # tf.add(f.name,os.path.join(f.subdir,os.path.basename(f.name)))
                logger.debug("Opening file for sandbox: %s" % f.name)
                try:
                    fileobj = open(f.name)
                except Exception as err:
                    raise SandboxError("File %s does not exist." % f.name)

                tinfo = tf.gettarinfo(f.name, os.path.join(f.subdir, os.path.basename(f.name)))

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
            tf = tarfile.open(tgzfile, "r:*")
        except tarfile.ReadError:
            logger.warning('Sandbox is empty or unreadable')
            return
        else:
            [tf.extractall(dest_dir, [tarinfo]) for tarinfo in tf]
            tf.close()


#####################################################
