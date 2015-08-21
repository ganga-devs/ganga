import datetime
import os
import random
import tempfile
import time

def generateUniqueTempFile( ext = '.txt' ):

    myFile = tempfile.NamedTemporaryFile(mode='w', delete=False)

    t = datetime.datetime.now()
    unix_t = time.mktime(t.timetuple())

    file_string = str(unix_t) + "\n"

    random.seed( unix_t )
    rand = random.random() * 1E10

    file_string = file_string + str( rand ) + "\n"

    urand = os.urandom(20)

    file_string = file_string + str( urand ) + "\n"

    myFile.write( file_string )

    myFile.close()

    returnableName = myFile.name+str(ext)

    os.rename( myFile.name, returnableName )

    return returnableName


