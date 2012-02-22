# GANGA Package for MedAustron
#
# Dietrich Liko, August 2009
#
import sys

print >>sys.stderr
print >>sys.stderr, "GangaMedAustron - https://cern.ch/twiki/bin/view/ArdaGrid/MedAustron"
print >>sys.stderr

def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}

def loadPlugins(c):
    pass
