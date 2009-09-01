# Create a file list to check transfer was correct

import sys, os

if len(sys.argv) != 2:
    print "ERROR: incorrect number of options given. Just give me the directory to list!"
    sys.exit()


for root, dirs, files in os.walk(sys.argv[1]):

    for file in files:
        print os.path.join(root, file) + "    " + str(os.path.getsize( os.path.join(root, file) ))
