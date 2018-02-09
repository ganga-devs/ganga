#!/usr/bin/env python

from __future__ import print_function

import sys, os
import logging
import logging.handlers
import traceback
import time
from ConfigParser import ConfigParser
import socket
import tempfile
import commands

# ------------------------------------------------
def writeHeartbeat( file, start_time ):
    # save daemon info
    f = open(file, "w")
    f.write("hostname:  %s\n" % socket.gethostname())
    f.write("pid:  %d\n" % os.getpid())
    f.write( time.strftime("Started server at: %a, %d %b %Y %H:%M:%S +0000\n", start_time) )
    f.write( time.strftime("Heartbeat: %a, %d %b %Y %H:%M:%S +0000\n", time.gmtime()) )
    f.close()

# ------------------------------------------------
def setupLogger( logname = '' ):
    "Setup the logger"
    global gLogger, gLogStreamHandler, gLogFormatter, gLogFileHandler
    
    if not gLogger:
        gLogger = logging.getLogger("gangaService")
        gLogStreamHandler = logging.StreamHandler()
        gLogger.setLevel(logging.DEBUG)
        gLogStreamHandler.setLevel(logging.WARNING)
        gLogFormatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        gLogStreamHandler.setFormatter(gLogFormatter)
        gLogger.addHandler(gLogStreamHandler)

    if logname:

        if gLogFileHandler:
            gLogger.removeHandler( gLogFileHandler )
            
        gLogFileHandler = logging.handlers.RotatingFileHandler(
            logname, maxBytes=10000000, backupCount=5)
        gLogFileHandler.setLevel(logging.DEBUG)
        gLogFileHandler.setFormatter(gLogFormatter)
        gLogFileHandler.doRollover()
        gLogger.addHandler(gLogFileHandler)

# ------------------------------------------------
def formatTraceback():
    "Helper function to printout a traceback as a string"
    return "\n %s\n%s\n%s\n" % (''.join( traceback.format_tb(sys.exc_info()[2])), sys.exc_info()[0], sys.exc_info()[1])
    
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
    
    if not gConfig.has_option('General', 'gangaExec'):
        gLogger.error("Config file does not contain a mandatory 'gangaExec' field.")
        return 1

    if not gConfig.has_option('General', 'serviceDir'):
        gLogger.error("Config file does not contain a mandatory 'serviceDir' field.")
        return 1

    if not gConfig.has_option('General', 'uiScript'):
        gLogger.error("Config file does not contain a mandatory 'uiScript' field.")
        return 1

    # update log file
    if gConfig.has_option('General', 'logFile'):
        setupLogger( gConfig.get('General', 'logFile') )

    return 0
    
# ------------------------------------------------
# start the GangaService
gLogger = None
gLogFileHandler = None
gLogStreamHandler = None
gLogFormatter = None
gConfig = ConfigParser()

os.environ['KRB5CCNAME'] = 'FILE:/afs/cern.ch/user/t/tagexsrv/kinit_ticket'
os.system("kinit -r 100d")

# check for service kill
if len(sys.argv) > 1 and sys.argv[1] == '-k':
    gConfig.read( "/afs/cern.ch/user/t/tagexsrv/public/GangaService-atlddm10/gangaService.ini" )
    #gConfig.read( "/home/mws/GangaService/gangaService.ini" )
    work_dir = os.path.abspath( os.path.expanduser( os.path.expandvars( gConfig.get('General', 'serviceDir') ) ) )
    stop_file = os.path.join(work_dir, "gangaService.stop")
    if gConfig.has_option('General', 'stopFile'):
        stop_file = os.path.join(work_dir, gConfig.get('General', 'stopFile') )
        
    open(stop_file, "w")
    print("Service stop file '%s' created..." % stop_file)
    sys.exit(0)
    
# setup the logger
setupLogger('gangaService.log')

# load up the config
if loadConfig("/afs/cern.ch/user/t/tagexsrv/public/GangaService-atlddm10/gangaService.ini" ):
#if loadConfig("/home/mws/GangaService/gangaService.ini" ):
    gLogger.error("Problem loading config file.")
    sys.exit(1)

#-------------------------------------------------------------
# startup daemon if required
if len(sys.argv) > 1 and sys.argv[1] == '-d':

    # fork the daemon
    gLogger.warning("Starting Daemon...")
    pid = os.fork()
    if pid < 0:
        gLogger.error("Could not fork a child process.")
        sys.exit(1)

    if pid > 0:
        print("Service created. Exiting parent...")
        sys.exit(0)
        
    os.umask(0)
    os.setsid()
    pid = os.fork()
    
    if pid > 0:
        print("Service created. Exiting parent...")
        sys.exit(0)
        
    # removing stream handler and close the streams
    gLogger.removeHandler(gLogStreamHandler)

    sys.stdout.flush()
    sys.stderr.flush()

    work_dir = os.path.abspath( os.path.expanduser( os.path.expandvars( gConfig.get('General', 'serviceDir') ) ) )
    
    si = file("/dev/null", 'r')
    so = file(os.path.join(work_dir, "gangaService.stdout"), 'w')
    se = file(os.path.join(work_dir, "gangaService.stderr"), 'w', 0)
    
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())
    
    #si = file("/dev/null", 'r')
    #so = file("/dev/null", 'a+')
    #se = file("/dev/null", 'a+', 0)
    
    #os.dup2(si.fileno(), sys.stdin.fileno())
    #os.dup2(so.fileno(), sys.stdout.fileno())
    #os.dup2(se.fileno(), sys.stderr.fileno())

    #sys.stdin.close()
    #sys.stdout.close()
    #sys.stderr.close()

    gLogger.debug("Daemon started.")

#-------------------------------------------------------------
# setup config info/defaults
start_time = time.gmtime()

# Setup work dir
work_dir = os.path.abspath( os.path.expanduser( os.path.expandvars( gConfig.get('General', 'serviceDir') ) ) )
gLogger.warning("Setting working directory to '%s'" % work_dir)
try:
    if not os.path.exists( work_dir ):
        os.makedirs( work_dir )
    os.chdir( work_dir )
except:
    gLogger.error("Could not create/change to given working directory: %s" % formatTraceback())
    sys.exit(1)

# setting proxy script
proxy_script = gConfig.get('General', 'uiScript')
gLogger.warning("Setting UI script to '%s'" % proxy_script)

# Setup proxy dir
cred_dir = work_dir
if gConfig.has_option('General', 'credentialsDir'):
    cred_dir = os.path.abspath( os.path.expanduser( os.path.expandvars( gConfig.get('General', 'credentialsDir') ) ) )
    
gLogger.warning("Setting credentials directory to '%s'" % cred_dir)
try:
    if not os.path.exists( cred_dir ):
        os.makedirs( cred_dir )
except:
    gLogger.error("Could not create the given credentials directory: %s" % formatTraceback())
    sys.exit(1)
                                
# set up users file
user_file = os.path.join(work_dir, "gangaService.users")
if gConfig.has_option('General', 'userFile'):
    user_file = gConfig.get('General', 'userFile')

gLogger.debug("Setting 'users' file to %s" % user_file)

if not os.path.exists( user_file ):
    try:
        open( user_file, "w")
    except:
        gLogger.error("Could not create active users file: %s" % formatTraceback())
        sys.exit(1)

# set the stop file
stop_file = os.path.join(work_dir, "gangaService.stop")
if gConfig.has_option('General', 'stopFile'):
    stop_file = os.path.join(work_dir, gConfig.get('General', 'stopFile') )
    
gLogger.debug("Setting 'stop' file to %s" % stop_file)
    
if os.path.exists(stop_file):
    try:
        os.remove(stop_file)
    except:
        gLogger.error("Could not remove service stop file: %s" % formatTraceback())
        sys.exit(1)

# set the pre script file
pre_script_file = os.path.join(work_dir, "gangaService_pre.py")
pre_script = ""
if gConfig.has_option('General', 'preScriptFile'):
    pre_script_file = gConfig.get('General', 'preScriptFile')

gLogger.debug("Setting 'pre-script' file to %s" % pre_script_file)

# set the post script file
post_script_file = os.path.join(work_dir, "gangaService_post.py")
post_script = ""
if gConfig.has_option('General', 'postScriptFile'):
    post_script_file = gConfig.get('General', 'postScriptFile')

gLogger.debug("Setting 'post-script' file to %s" % post_script_file)

# set the heartbeat file
heartbeat_file = os.path.join(work_dir, "gangaService.info")
if gConfig.has_option('General', 'heartbeatFile'):
    heartbeat_file = gConfig.get('General', 'heartbeatFile')

gLogger.debug("Setting 'heartbeat' file to %s" % heartbeat_file)
try:
    writeHeartbeat( heartbeat_file, start_time )
except:
    gLogger.error("Could not write heartbeat file '%s': %s" % (heartbeat_file, formatTraceback()))
    sys.exit(1)
    
# set the sleep time
sleep_time = 60
if gConfig.has_option('General', 'sleepTime'):
    try:
        sleep_time = eval( gConfig.get('General', 'sleepTime') )
    except:
        gLogger.error("Could not set sleep time: %s" % formatTraceback())
        sys.exit(1)
        
gLogger.debug("Setting 'sleepTime' to %d" % sleep_time)

# set the script lifetime
script_lifetime = 24
if gConfig.has_option('General', 'scriptLifetime'):
    try:
        script_lifetime = eval( gConfig.get('General', 'scriptLifetime') )
    except:
        gLogger.error("Could not set script lifetime: %s" % formatTraceback())
        sys.exit(1)
        
gLogger.debug("Setting 'scriptLifetime' to %d" % script_lifetime)

        
#-------------------------------------------------------------
# main daemon loop
while True:

    # check the active users file
    active_users = []
    try:
        f = open(user_file)
        for ln in f.readlines():
            active_users.append( ln.strip() )
    except:
        gLogger.error("Could not load active users file: %s" % formatTraceback())
        break

    # check for new scripts in the user directories
    for dir in os.listdir( work_dir ):

        if not os.path.exists( os.path.join( work_dir, dir, 'jobs_requests' ) ):
            continue

        if len(os.listdir( os.path.join( work_dir, dir, 'jobs_requests' ) ) ) > 0 and not dir in active_users:
            active_users.append(dir)

    # loop over active users
    not_active = []
    for user in active_users:

        jobs_requests_dir = os.path.join( work_dir, user, 'jobs_requests' )
        scripts_dir = os.path.join( work_dir, user, 'scripts' )
        gangadir_dir = os.path.join( work_dir, user, 'gangadir' )
        info_dir = os.path.join( work_dir, user, 'info' )
        proxy_file = os.path.join( cred_dir, user, 'credentials', 'proxy' )
        pre_script_file_user = os.path.join( work_dir, user, 'pre_post', pre_script_file)
        post_script_file_user = os.path.join( work_dir, user, 'pre_post', post_script_file)
        
        gLogger.debug("Active user '%s' found" % user)

        # check proxy
        cmd = "source %s && voms-proxy-info -file %s --timeleft" % (proxy_script, proxy_file)
        (status, output) = commands.getstatusoutput( cmd )
        if status:
            gLogger.warning("Problem checking proxy file for user %s: %s" % (user, formatTraceback()))
            gLogger.warning("Command used: " + cmd)
            continue

        lines = output.split('\n')
        lifetime = eval(lines[ len(lines) - 1 ])
        if lifetime < 6 * 60 * 60:
            gLogger.warning("Proxy for user %s less than 6 hours." % user)
            continue
        
        # create associated directories
        if not os.path.exists(jobs_requests_dir):
            try:
                os.makedirs(jobs_requests_dir)
            except:
                gLogger.error("Could not create job requests dir for user %s: %s" % (user, formatTraceback()))
                continue

        if not os.path.exists(scripts_dir):
            try:
                os.makedirs(scripts_dir)
            except:
                gLogger.error("Could not create scripts dir for user %s: %s" % (user, formatTraceback()))
                continue

        if not os.path.exists(info_dir):
            try:
                os.makedirs(info_dir)
            except:
                gLogger.error("Could not create info dir for user %s: %s" % (user, formatTraceback()))
                continue

        # clean script dir if required
        gLogger.debug("Cleaning script directoy '%s'" % scripts_dir)
        for sc in os.listdir( scripts_dir ):
            try:
                if os.path.getctime( os.path.join( scripts_dir , sc ) ) < time.time() - script_lifetime * 60 * 60:

                    os.remove( os.path.join( scripts_dir , sc ) )
            except:
                gLogger.warning("Problems cleaning '%s' from scripts dir: %s" % ( os.path.join( scripts_dir , sc ), formatTraceback()))

        # check for new job requests - note only one at a time!
        script = ""
        for fn in os.listdir( jobs_requests_dir ):
            if fn[0] == '.':
                continue
            
            gLogger.debug("Adding script '%s'" % os.path.join( jobs_requests_dir, fn ))
            try:
                script += open( os.path.join( jobs_requests_dir, fn ) ).read()
            except:
                gLogger.error("Could not open script %s: %s" % (fn, formatTraceback()))

            try:
                os.remove( os.path.join( jobs_requests_dir, fn ) )
            except:
                gLogger.error("Could not remove script %s: %s" % (fn, formatTraceback()))
                
            break

        # load the pre and post scripts for this user
        pre_script = ""
        if not os.path.exists( pre_script_file_user ):
            gLogger.warning("Pre-script file '%s' not found." % pre_script_file_user )
        else:
            try:
                pre_script = open(pre_script_file_user).read()
            except:
                gLogger.warning("Pre-Script file '%s' could not be opened: %s" % (pre_script_file_user, formatTraceback()))

        post_script = ""
        if not os.path.exists( post_script_file_user ):
            gLogger.warning("Post-script file '%s' not found." % post_script_file_user )
        else:
            try:
                post_script = open(post_script_file_user).read()
            except:
                gLogger.warning("Post-Script file '%s' could not be opened: %s" % (post_script_file_user, formatTraceback()))
                
        # create ganga script
        full_script = "_user = '%s'\n_infodir = '%s'\n" % (user, info_dir )
        full_script += pre_script + "\n" + script + "\n" + post_script

        if script == '':
            full_script += """
active = False
for j in jobs:
   if not j.status in ['completed','failed','killed','new']:
      active = True

if not active:
   print("NOTACTIVE")
"""
        gLogger.warning("Preparing run script...")
        
        try:
            script_fd, script_path = tempfile.mkstemp( dir = scripts_dir ) 
            script_file = os.fdopen( script_fd, "w" )
            script_file.write( full_script )
            script_file.close()
        except:
            gLogger.error("Couldn't store script to path '%s': %s" % (script_path, formatTraceback()))
            continue
        
        # run ganga
        cmd = "cp notifyUser.py "+scripts_dir+"; export X509_USER_PROXY=" + proxy_file + " ; " + gConfig.get('General', 'gangaExec') + " -o'[Configuration]gangadir=%s' -o'[defaults_GridProxy]minValidity=00:15' -o'[Configuration]repositorytype=LocalXML' %s" % (gangadir_dir, script_path)
        gLogger.warning("Running Ganga with command:  %s" % cmd )
        
        (status, output) = commands.getstatusoutput( cmd )
        
        if output.find("command not found") != -1 or output.find("Error") != -1 or output.find("Traceback") != -1:
            gLogger.warning("Problem running ganga: %s" % output)
            continue
        else:
            gLogger.debug("Ganga completed successfully.")

        # remove from active users
        if output.find("NOTACTIVE") != -1:
            not_active.append(user)

    # amend active users list
    f = open(user_file, "w")
    for user in active_users:
        if not user in not_active:
            f.write( user + '\n')
            
    f.close()
                
    # check for halt
    if os.path.exists(stop_file):
        break

    # ping the heartbeat file
    try:
        writeHeartbeat(heartbeat_file, start_time)
        print("HEARTBEAT:  " + time.asctime())
    except:
        gLogger.warning("Could not write heartbeat file %s: %s" % (heartbeat_file, formatTraceback()))


    # refresh AFS
    os.system("/usr/sue/bin/kinit -R")
    
    # sleep for a bit
    time.sleep(sleep_time)

    sys.stdout.flush()
    sys.stderr.flush()
        
# finished
gLogger.debug("Service finished cleanly")
