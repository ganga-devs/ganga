
def generateUniqueTempFile():

    import tempfile

    myFile = tempfile.NamedTemporaryFile(mode='w',delete=False)

    import datetime, time
    t = datetime.datetime.now()
    unix_t = time.mktime(t.timetuple())

    file_string = str(unix_t) + "\n"

    import random
    random.seed( unix_t )
    rand = random.random() * 1E10

    file_string = file_string + str( rand ) + "\n"

    import os
    urand = os.urandom(20)

    file_string = file_string + str( urand ) + "\n"

    myFile.write( file_string )

    myFile.close()

    return myFile.name

