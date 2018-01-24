
from GangaCore.Core.exceptions import GangaException
from GangaCore.GPIDev.Lib.File.File import File
from GangaCore.GPIDev.Lib.File.LocalFile import LocalFile

import GangaCore.Utility.logging
import fnmatch

import os

logger = GangaCore.Utility.logging.getLogger()

transformDictionary = {}

def loadScript(scriptFilePath, indentation):

    if not os.path.exists(scriptFilePath):
        from GangaCore.Core.exceptions import GangaException
        raise GangaException("Error Finding script file: %s" % str(scriptFilePath))

    with open(scriptFilePath) as this_file:
        file_data = this_file.read()

    return indentScript(file_data, indentation)

def indentScript(script, indenting):

    script_lines = script.split('\n')
    output_script = []
    for this_line in script_lines:
        output_script.append(str(indenting) + str(this_line))

    return '\n'.join(output_script)

def __populate():
    if len(transformDictionary) == 0:
        transformDictionary[File] = LocalFile
        # transformDictionary[ ] =
__populate()

def safeTransformFile(input_file):

    this_type = type(input_file)
    if this_type in transformDictionary:
        return transformDictionary[this_type](input_file)

    logger.error("Cannot safely transform file of type: %s" % (this_type))

    return None

def doesFileExist( input_file=None, input_list = None ):

    if input_file is None:
        input_file = 'some.file'
    if input_list is None:
        input_list = []

    test_fileName = ''
    if type(input_file) == type(''):
        test_fileName = input_file
    elif hasattr(input_file, 'namePattern'):
        test_fileName = input_file.namePattern
    else:
        raise GangaException("Failed to understand file: %s" % str(input_file))

    have_matched = False
    for input_fileName in input_list:
        check_str = ''
        if type(input_fileName) == type(''):
            check_str = input_fileName
        elif hasattr(input_fileName, 'namePattern'):
            check_str = input_fileName.namePattern
        else:
            raise GangaException("Failed to understand file or pattern: %s" % str(input_fileName))

        if fnmatch.fnmatch( test_fileName, check_str ):
            have_matched = True
            break

    return have_matched

