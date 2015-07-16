
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File.LocalFile import LocalFile

transformDictionary = {}


def __populate():
    if len(transformDictionary) == 0:
        transformDictionary[type(File())] = LocalFile
        # transformDictionary[ ] =
__populate()


def safeTransformFile(input_file):

    this_type = type(input_file)
    if this_type in transformDictionary:
        return transformDictionary[this_type](input_file)

    logger.error("Cannot safely transform file of type: %s" % (this_type))

    return None

def doesFileExist( input_file='some.file', input_list = [] ):

    import fnmatch

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

