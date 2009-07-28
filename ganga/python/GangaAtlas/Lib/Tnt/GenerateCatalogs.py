#!/usr/bin/env python

# GenerateCatalogs.py - methods to look up file locations and generate
# POOL XML file catalogues with physical and logical names
                                                                                                      
import xml.dom
import xml.dom.minidom
import getopt
import os
import re
import sys
import commands
import random
from Ganga.Utility.logging import getLogger
logger = getLogger()

try:
    import lfc
except ImportError:
    logger.debug('Error importing lfc module')
    pass

try:
    from dq2.clientapi.DQ2 import DQ2
    from dq2.common.client.x509 import *
    from dq2.common.constants import *
    from dq2.common.DQException import *
    from dq2.common.utils.ping import *
    from dq2.location.client.LocationClient import LocationClient
except ImportError:
    logger.warning("Environment not set [error importing DQ2 dependencies]!")
    pass

def getPFNsFromLFC(guidLfnMap):
    """Check LFC for local site and find the PFNs of the required files.
    """
#    print "Getting PFNs from LFC..."
    if os.environ['LFC_HOST'] == '':
        raise ConfigError("Error: LFC_HOST is not set! Exiting now...")

    try:
        pfns = {}
        lfc.lfc_startsess('','')
        for guid, lfn in guidLfnMap.iteritems():
            rc,frs = lfc.lfc_getreplica('',guid,'')
            if rc == 0 and frs != None and len(frs) != 0:
                # get SURL
                sfn = frs[0].sfn
                                                                                                      
            # remove protocol and host
#TODO       # currently hardwired for local access - need to make this more flexible
#            pfn = re.sub('^[^:]+://[^/]+','rfio:',sfn)
                pfn = 'file:///tmp/aod/'+lfn
                                                                                                      
                pfns[lfn] = pfn
        lfc.lfc_endsess()
    except:
        logger.warning("Error in getPFNsFromLFC")
        pass

    return pfns

def generateCatalog(guidLfnMap, lfnPfnMap, pfcName):
    """Generate a local POOL XML file catalogue from the given files.
       Heavily based on code from  dq2_poolFCjobO script by Tadashi Maeno.
    """
#    print "Generating POOL catalogue..."
    # header
    header = \
"""<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
                                                                                                      
<!-- Edited By POOL -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>
"""
    # item
    item = \
"""
  <File ID="%s">
    <physical>
      <pfn filetype="ROOT_All" name="%s"/>
    </physical>
    <logical>
      <lfn name="%s"/>
    </logical>
  </File>
"""
    # trailer
    trailer = \
"""
</POOLFILECATALOG>
"""
    # check if PoolFileCatalog exists
    oldXML = []
    oldGUIDs = []
    if os.path.exists(pfcName):
        # read lines
        inFile = open(pfcName)
        oldXML = inFile.readlines()
        inFile.close()
        # extract GUIDs
        # rename
        os.rename(pfcName,pfcName+'.BAK')
    # open
    outFile = open(pfcName,'w')
    # write header
    outFile.write(header)
    # write files
    newGUIDs = []
    for file in guidLfnMap.iteritems():
        outFile.write(item % (file[0].upper(),lfnPfnMap.get(file[1]),file[1]))
        newGUIDs.append(file[0].upper())
    # write old files
    fileFlag = False
    for line in oldXML:
        # look for file item
        match = re.search('<File ID="([^"]+)"',line)
        if match != None:
            # avoid duplication
            guid = match.group(1)
            if not guid in newGUIDs:
                fileFlag = True
                outFile.write('\n')
            else:
                logger.warning( "WARNING: duplicated GUID %s in %s. Replaced" % (guid,pfcName))
        # write
        if fileFlag:
            outFile.write(line)
        # look for item end
        if re.search('</File>',line) != None:
            fileFlag = False
    # write trailer
    outFile.write(trailer)
    outFile.close()


