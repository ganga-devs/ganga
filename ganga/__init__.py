import os
import sys

pth = os.path.join(os.path.dirname(os.path.realpath(__file__)))
print pth
sys.path.insert(0, pth)
print __name__

#del sys.modules[__name__] 
#import ganga


#This is a little cheat so that ganga can be imported with import ganga in a python session, rather than import ganga.ganga
#def pathSetup():
#    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

#pathSetup()
#import ganga
