
import random
import tempfile
import string

def generateUniqueTempFile(ext = '.txt'):
    """ Generate a unique file with a given filename with some random contents and return the name of the file on disk
    Args:
        ext (str): This is the extension (including '.') to give to the file of interest
    """

    with tempfile.NamedTemporaryFile(mode='w',suffix=ext,delete=False) as myFile:

        myFile.write(''.join(random.choice(string.ascii_uppercase+string.digits) for _ in range(20)))

    return myFile.name


