#!/usr/bin/env python

import sys, os
import logging
import logging.handlers
import traceback
import time
from ConfigParser import ConfigParser
import socket
import tempfile
import commands
import shutil
import stat

# ------------------------------------------------
def setupLogger( ):
    "Setup the logger"
    global gLogger, gLogFormatter, gLogStreamHandler
    
    if not gLogger:
        gLogger = logging.getLogger("gangaService")
        gLogStreamHandler = logging.StreamHandler()
        gLogger.setLevel(logging.DEBUG)
        gLogStreamHandler.setLevel(logging.DEBUG)
        gLogFormatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        gLogStreamHandler.setFormatter(gLogFormatter)
        gLogger.addHandler(gLogStreamHandler)

# ------------------------------------------------
def loadConfig( fname ):
    "load in the given config"
    global gConfig
    
    # check config is present
    if not os.path.exists( fname ):
        gLogger.error("Config file %s does not exist." % fname )
        return 1

    # read it in
    try:
        gConfig.read( fname )
    except:
        gLogger.error("Error reading config file: %s" % formatTraceback())
        return 1

    # check mandatory entries
    if not gConfig.has_section('General'):
        gLogger.error("Config file does not contain a mandatory 'General' section.")
        return 1
    
    if not gConfig.has_option('General', 'serviceDir'):
        gLogger.error("Config file does not contain a mandatory 'serviceDir' field.")
        return 1
    
# ------------------------------------------------
def formatTraceback():
    "Helper function to printout a traceback as a string"
    return "\n %s\n%s\n%s\n" % (''.join( traceback.format_tb(sys.exc_info()[2])), sys.exc_info()[0], sys.exc_info()[1])
    
# ------------------------------------------------
# setup
gLogger = None
gLogStreamHandler = None
gLogFormatter = None
gConfig = ConfigParser()

# setup the logger
setupLogger()

# check args
if len(sys.argv) != 4:
    gLogger.error("Incorrect arguments specified. Usuage:\nsubmitJob.py <username> <jobscript> <proxyfile>")
    sys.exit(1)

user = sys.argv[1]
script_file = sys.argv[2]
proxy_file = sys.argv[3]

# load up the config
if loadConfig("/afs/cern.ch/user/t/tagexsrv/public/GangaService-atlddm10/gangaService.ini" ):
#if loadConfig("/home/mws/GangaService/gangaService.ini" ):
    gLogger.error("Problem loading config file.")
    sys.exit(1)

work_dir = os.path.abspath( os.path.expanduser( os.path.expandvars( gConfig.get('General', 'serviceDir') ) ) )
gLogger.warning("Setting working directory to '%s'" % work_dir)
try:
    if not os.path.exists( work_dir ):
        os.makedirs( work_dir )
except:
    gLogger.error("Could not create/change to given working directory: %s" % formatTraceback())
    sys.exit(1)

         
# add script to job request directory
jobs_requests_dir = os.path.join( work_dir, user, 'jobs_requests' )
if os.path.exists( os.path.join( jobs_requests_dir, os.path.basename(script_file) ) ):
    gLogger.error("Could not copy script file '%s' due to the file already existing." % os.path.basename(script_file))
    sys.exit(1)
    
try:
    if not os.path.exists(jobs_requests_dir):
        os.makedirs(jobs_requests_dir)
        
    shutil.copy(script_file, os.path.join( jobs_requests_dir, os.path.basename(script_file) ) )
except:
    gLogger.error("Could not copy script file '%s': %s" % (os.path.basename(script_file), formatTraceback()))
    sys.exit(1)

# add proxy to the directory
cred_dir = work_dir
if gConfig.has_option('General', 'credentialsDir'):
    cred_dir = os.path.abspath( os.path.expanduser( os.path.expandvars( gConfig.get('General', 'credentialsDir') ) ) )
credentials_dir = os.path.join( cred_dir, user, 'credentials' )
try:
    if not os.path.exists(credentials_dir):
        os.makedirs(credentials_dir)

    shutil.copy(proxy_file, os.path.join( credentials_dir, 'proxy' ) )
    os.system('chmod 600 ' + os.path.join( credentials_dir, 'proxy' ) )
    
except:
    gLogger.error("Could not copy proxy file '%s': %s" % (os.path.basename(proxy_file), formatTraceback()))
    sys.exit(1)
                                
            
gLogger.debug("Job request successfully submitted")
