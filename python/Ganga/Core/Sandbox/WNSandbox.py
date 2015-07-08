"""
Sandbox functions used in the job wrapper script on the worker node.
The text of this module is sourced into the job wrapper script.
It therefore may use ###TAGS###  which are expanded in the wrapper script.
"""

INPUT_TARBALL_NAME = '_input_sandbox.tgz'
OUTPUT_TARBALL_NAME = '_output_sandbox.tgz'
PYTHON_DIR = '_python'

import os

import tarfile


def getPackedInputSandbox(tarpath, dest_dir='.'):
    """Get all sandbox_files from tarball and write them to the workdir.
       This function is called by wrapper script at the run time.
    Arguments:
      'tarpath': a path to the tarball
      'dest_dir': a destination directory
    """

    with tarfile.open(tarpath, "r:gz") as tf:
        tf.extractall(dest_dir)


def createOutputSandbox(output_patterns, filter, dest_dir):
    """Get all files matching output patterns except filtered with filter and
       write them to the destination directory.
       This function is called by wrapper script at the run time.
    Arguments:
      'output_patterns': list of filenames or patterns.
      'filter': function to filter files (return True to except) 
      'dest_dir': destination directory for output files
    """

    try:
        from Ganga.Utility.files import multi_glob, recursive_copy
    except IOError as e:
        import sys

        print("Failed to import files")
        print("sys:")
        print(sys.path)
        print("env:")
        print(os.environ)
        print("ls:")
        print(os.listdir("."))
        print("pattern:")
        print(output_patterns)
        print("destdir:")
        print(dest_dir)

        try:
            import traceback
            traceback.print_stack()
        except:
            pass

        print("Trying fix")
        sys.path.insert(0, os.path.join(os.getcwd(), PYTHON_DIR))

        try:
            from Ganga.Utility.files import multi_glob, recursive_copy
            print("Success!")
        except IOError as e:
            print("Fail!")
            raise e

    for f in multi_glob(output_patterns, filter):
        try:
            recursive_copy(f, dest_dir)
        except Exception as x:
            print("ERROR: (job ###JOBID### createOutput )", x)


def createPackedOutputSandbox(output_patterns, filter, dest_dir):
    """Get all files matching output patterns except filtered with filter and
       put them to the Sandbox tarball in destination directory.
       This function is called by wrapper script at the run time.
    Arguments:
      'output_patterns': list of filenames or patterns.
      'filter': function to filter files (return True to except) 
      'dest_dir': destination directory for tarball
    """

    tgzfile = os.path.join(dest_dir, OUTPUT_TARBALL_NAME)

    try:
        from Ganga.Utility.files import multi_glob, recursive_copy
    except IOError as e:
        import sys

        print("Failed to import files")
        print("sys:")
        print(sys.path)
        print("env:")
        print(os.environ)
        print("ls:")
        print(os.listdir("."))
        print("pattern:")
        print(output_patterns)
        print("destdir:")
        print(dest_dir)

        try:
            import traceback
            traceback.print_stack()
        except:
            pass

        print("Trying fix")
        sys.path.insert(0, os.path.join(os.getcwd(), PYTHON_DIR))

        try:
            from Ganga.Utility.files import multi_glob, recursive_copy
            print("Success!")
        except IOError as e:
            print("Fail!")
            raise e

    outputlist = multi_glob(output_patterns, filter)

    if outputlist:
        with tarfile.open(tgzfile, "w:gz") as tf:
            tf.dereference = True
            for f in outputlist:
                tf.add(f)
