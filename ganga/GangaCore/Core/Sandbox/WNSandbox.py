"""
Sandbox functions used in the job wrapper script on the worker node.
The text of this module is sourced into the job wrapper script.
It therefore may use ###TAGS###  which are expanded in the wrapper script.
"""

INPUT_TARBALL_NAME = '_input_sandbox.tgz'
OUTPUT_TARBALL_NAME = '_output_sandbox.tgz'
PYTHON_DIR = '_python'

import os
import mimetypes

import tarfile
from contextlib import closing

def multi_glob(pats, exclude=None):
    """ glob using a list of patterns and removing duplicate files, exclude name in the list for which the callback exclude(name) return true
    example: advanced_glob(['*.jpg','*.gif'],exclude=lambda n:len(n)>20) return a list of all JPG and GIF files which have names shorter then 20 characters
    """

    unique = {}
    if exclude is None:
        def exclude(n): return 0

    for p in pats:
        for f in glob.glob(p):
            unique[f] = 1

    return [name for name in unique.keys() if not exclude(name)]


def recursive_copy(src, dest):
    """ copy src file (or a directory tree if src specifies a directory) to dest directory. dest must be a directory and must exist.
    if src is a relative path, then the src directory structure is preserved in dest.
    """

    if not os.path.isdir(dest):
        raise ValueError(
            'resursive_copy: destination %s must specify a directory (which exists)' % dest)

    if os.path.isdir(src):
        destdir = dest
        srcdir, srcbase = os.path.split(src.rstrip('/'))
        if not srcdir == '' and not os.path.isabs(src):
            destdir = os.path.join(destdir, srcdir)
            if not os.path.isdir(destdir):
                os.makedirs(destdir)
        shutil.copytree(src, os.path.join(destdir, srcbase))
    else:

        srcdir = os.path.dirname(src.rstrip('/'))
        if srcdir == '' or os.path.isabs(src):
            shutil.copy(src, dest)
        else:
            destdir = os.path.join(dest, srcdir)
            if not os.path.isdir(destdir):
                os.makedirs(destdir)
            shutil.copy(src, destdir)

def getPackedInputSandbox(tarpath, dest_dir='.'):
    """Get all sandbox_files from tarball and write them to the workdir.
       This function is called by wrapper script at the run time.
    Arguments:
      'tarpath': a path to the tarball
      'dest_dir': a destination directory
    """

    try:
        with closing(tarfile.open(tarpath, "r:*")) as tf:
            tf.extractall(dest_dir)
    except:
        raise Exception("Error opening tar file: %s" % tarpath)


def createOutputSandbox(output_patterns, filter, dest_dir):
    """Get all files matching output patterns except filtered with filter and
       write them to the destination directory.
       This function is called by wrapper script at the run time.
    Arguments:
      'output_patterns': list of filenames or patterns.
      'filter': function to filter files (return True to except) 
      'dest_dir': destination directory for output files
    """

    for f in multi_glob(output_patterns, filter):
        try:
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
            recursive_copy(f, dest_dir)
        except Exception as x:
            print("ERROR: (job ###JOBID### createOutput )", x)


def createPackedOutputSandbox(output_patterns, _filter, dest_dir):
    """Get all files matching output patterns except filtered with filter and
       put them to the Sandbox tarball in destination directory.
       This function is called by wrapper script at the run time.
    Arguments:
      'output_patterns': list of filenames or patterns.
      'filter': function to filter files (return True to except) 
      'dest_dir': destination directory for tarball
    """

    tgzfile = os.path.join(dest_dir, OUTPUT_TARBALL_NAME)

    outputlist = multi_glob(output_patterns, _filter)

    if outputlist:
        if mimetypes.guess_type(tgzfile)[1] in ['gzip']:
            file_format = 'gz'
        elif mimetypes.guess_type(tgzfile)[1] in ['bzip2']:
            file_format = 'bz2'
        else:
            file_format = ''
        with closing(tarfile.open(tgzfile, "w:%s" % file_format)) as tf:
            tf.dereference = True
            for f in outputlist:
                tf.add(f)

