import os
import sys
#This is a little cheat so that ganga can be imported with import ganga in a python session, rather than import ganga.ganga
def pathSetup():
    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

pathSetup()
import ganga
