###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: root.py,v 1.1 2008-07-17 16:41:01 moscicki Exp $
###############################################################################

from Ganga.Utility.Config import getConfig, ConfigError
from commands import getstatusoutput    
import Ganga.Utility.logging
import os

#
#       MOVED to Ganga/Lib/Root/Root.py
#
#config.setDefaultOptions({'location':'/afs/cern.ch/sw/lcg/external/root',
#                          'version':'5.14.00d',
#                          'arch':'slc3_ia32_gcc323',
#                          'path':'',
#                          'pythonhome':sys.prefix,
#                          'pythonversion':''})

def getrootsys(version = None, arch = None):
    rootsys = ""
    try:
        configroot = getConfig('ROOT')
        if version == None:
            rootver = configroot['version']
        else:
            rootver = str(version)
        if arch == None:
            rootarch = configroot['arch']
        else:
            rootarch = str(arch)
        if configroot['path']!="":
            rootsys = configroot['path']+"/"
        else:
            rootsys = configroot['location']+"/"+rootver+"/"+rootarch
            if os.path.exists( rootsys+"/root/" ):
                rootsys = rootsys+"/root/"
    except ConfigError:
        pass
    logger.debug("ROOTSYS: %s", rootsys)
        
    return rootsys

def getenvrootsys():
    """Determine and return $ROOTSYS environment variable"""
    import os
    try:
        rootsys = os.environ['ROOTSYS']
    except KeyError:
        rootsys=""
    return rootsys

def getpythonhome(arch = None, pythonversion=None):
    """Looks for the PYTHONHOME for the particular version and arch"""
    pythonhome = ''
    try:
        #returns a copy
        configroot = getConfig('ROOT').getEffectiveOptions()
        if arch != None:
            configroot['arch'] = arch
        if pythonversion != None:
            configroot['pythonversion'] = pythonversion
        #allow other Root variables to be used in the definition
        pythonhome = configroot['pythonhome']
        #supports ${foo} type variable expansion
        for k in configroot.keys():
            pythonhome = pythonhome.replace('${%s}' % k, configroot[k])
    except ConfigError:
        pass
    logger.debug('PYTHONHOME: %s', pythonhome)
    return pythonhome

def getenvpythonhome():
    """Deterimin the PYTHONHOME environment variable"""
    import os
    pythonhome = ''
    try:
        pythonhome = os.environ['PYTHONHOME']
    except KeyError:
        pass
    return pythonhome

def getconfrootsys():
    """Determine and return ROOTSYS from ganga configuration"""
    return Ganga.Utility.root.getrootsys()
    
def getrootprefix(rootsys = None):
    """Determine ROOT path and return prefix,
    emtpy if ROOT is not found in path or ERROR,
    else ROOTSYS+LD_LIBRARY_PATH+prefix
    """
    rc = 0
    if rootsys == None:
        rootsys = Ganga.Utility.root.getconfrootsys()
        if rootsys=="":
            rootsys = Ganga.Utility.root.getenvrootsys()
            if rootsys=="":
                logger.error("No proper ROOT setup")
                rc = 1
                
    rootprefix = "ROOTSYS="+rootsys+" LD_LIBRARY_PATH="+rootsys+"/lib:$LD_LIBRARY_PATH "+rootsys+"/bin/"
    logger.debug("ROOTPREFIX: %s", rootprefix)

    return rc, rootprefix

def checkrootprefix(rootsys = None):
    """Check if rootprefix variable holds valid values"""

    rc, rootprefix = Ganga.Utility.root.getrootprefix(rootsys)
    
    cmdtest = rootprefix + "root-config --version"
    rc, out = getstatusoutput(cmdtest)
    if (rc!=0):
        logger.error("No proper ROOT setup")
        logger.error("%s", out)
        return 1
    else:
        logger.info("ROOT Version: %s", out)
        return 0


logger = Ganga.Utility.logging.getLogger()

# $Log: not supported by cvs2svn $
# Revision 1.8.24.1  2007/10/12 13:56:28  moscicki
# merged with the new configuration subsystem
#
# Revision 1.8.26.1  2007/10/09 13:46:22  roma
# Migration to new Config
#
# Revision 1.8  2007/04/13 11:26:28  moscicki
# root version upgrade to 5.14.00d from Will
#
# Revision 1.7  2007/04/12 10:22:55  moscicki
# root version upgrade to 5.14.00b from Will
#
# Revision 1.6  2007/03/14 12:15:14  moscicki
# patches from Will
#
# Revision 1.5  2006/08/08 14:07:45  moscicki
# config fixes from U.Egede
#
# Revision 1.4  2006/08/01 10:05:59  moscicki
# changes from Ulrik
#
# Revision 1.3  2006/06/21 11:50:23  moscicki
# johannes elmsheuser:
#
# * more modular design and a few extentions
# * get $ROOTSYS from configuration or environment
#
# Revision 1.1  2006/06/13 08:46:56  moscicki
# support for ROOT
#
