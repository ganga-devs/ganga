##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: files.py,v 1.1 2008-07-17 16:41:00 moscicki Exp $
##########################################################################

"""
Helper functions for operations on files.
"""

import os
import glob
import stat
import shutil

_stored_expanded_paths = {}
_stored_full_paths = {}

def expandfilename(filename, force=False):
    """expand a path or filename in a standard way so that it may contain ~ and ${VAR} strings"""
    if filename in _stored_expanded_paths:
        expanded_path = _stored_expanded_paths[filename]
    else:
        expanded_path = os.path.expandvars(os.path.expanduser(filename))
        _stored_expanded_paths[filename] = expanded_path
    if os.path.exists(expanded_path) or force:
        return expanded_path

    return filename


def fullpath(path, force=False):
    """expandfilename() and additionally: strip leading and trailing whitespaces and expand symbolic links"""
    if path in _stored_full_paths:
        full_path = _stored_full_paths[path]
    else:
        full_path = os.path.realpath(expandfilename(path.strip(), True))
        _stored_full_paths[path] = full_path
    if os.path.exists(full_path) or force:
        return full_path

    return path


def previous_dir(path, cnt):
    "returns the previous cnt-th directory"
    for i in range(cnt):
        path = os.path.dirname(path)
    return path


def chmod_executable(path):
    "make a file executable by the current user (u+x)"
    os.chmod(path, stat.S_IXUSR | os.stat(path).st_mode)


def is_executable(path):
    "check if the file is executable by the current user (u+x)"
    return os.stat(path)[0] & stat.S_IXUSR


def real_basename(x):
    """ a 'better' basename (removes the trailing / like on Unix) """
    x = x.rstrip(os.sep)
    return os.path.basename(x)


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


def remove_prefix(fn, path_list):
    """Remove the common prefix of fn and the first matching element in the path_list.

    Example: for fn='/a/b/c' and path=['d','/a','/a/b'] return 'b/c'

    If no matching path is found, then fn is returned unchanged.

    This function converts each element of the path_list using realpath.abspath.

    """
    for p in path_list:
        # normalize path
        if not p or p == '.':
            continue
        p = os.path.realpath(os.path.abspath(p))
        # print 'searching path component: %s'%p
        idx = fn.find(p)
        if idx != -1:
            # print 'found "%s" atr index %d in "%s"'%(p,idx,fn)
            return fn[len(p) + len(os.sep):]

    return fn

if __name__ == "__main__":

    workdir = 'test_recursive_copy'
    shutil.rmtree(workdir, True)
    os.mkdir(workdir)
    os.chdir(workdir)

    destdir = 'dest'
    os.mkdir(destdir)

    # Copy file
    os.mknod('file1')
    recursive_copy('file1', destdir)
    assert os.path.isfile(destdir + '/file1')

    # Copy file in dir
    os.mkdir('src')
    os.mknod('src/file2')
    recursive_copy('src/file2', destdir)
    assert os.path.isfile(destdir + '/src/file2')

    # Copy file in dir
    #       Check possibility to copy to existing dir
    os.mknod('src/file3')
    recursive_copy('src/file3', destdir)
    assert os.path.isfile(destdir + '/src/file3')

    # Copy file with abs path
    os.mknod('file4')
    recursive_copy(os.getcwd() + '/file4', destdir)
    assert os.path.isfile(destdir + '/file4')

    # Copy subdir
    os.mkdir('src/subdir')
    os.mknod('src/subdir/file5')
    recursive_copy('src/subdir', destdir)
    assert os.path.isfile(destdir + '/src/subdir/file5')

    # Copy subdir
    #       Check "/" at the end
    os.mkdir('src/subdir2')
    os.mknod('src/subdir2/file6')
    recursive_copy('src/subdir2/', destdir)
    assert os.path.isfile(destdir + '/src/subdir2/file6')

    # Copy subdir with abs path
    os.mkdir('src/subdir3')
    os.mknod('src/subdir3/file7')
    recursive_copy(os.getcwd() + '/src/subdir3', destdir)
    assert os.path.isfile(destdir + '/subdir3/file7')
