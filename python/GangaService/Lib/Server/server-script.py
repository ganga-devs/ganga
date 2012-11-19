import os
import sys
import pickle
import traceback
import StringIO
import time
from socket import *
import traceback
import threading
from Ganga.Utility.logging import getLogger

from Ganga.GPI import jobs
from Ganga.GPI import tasks

class WatchdogThread ( threading.Thread ):

    def __init__(self):
        super(WatchdogThread,self).__init__()
        self.running = True

    def run ( self ):
        # update the server file every 10s
        while self.running:
            open( os.path.join(config["Configuration"]["gangadir"], "server", "server.info"), "w").write(os.uname()[1])
            time.sleep(10)
        
def formatTraceback():
    "Helper function to printout a traceback as a string"
    return "\n %s\n%s\n%s\n" % (''.join( traceback.format_tb(sys.exc_info()[2])), sys.exc_info()[0], sys.exc_info()[1])
        
# get the port from the config
port = config['Configuration']['ServerPort']

# try and connect to this port
addr = ('localhost',port)
sock = socket(AF_INET,SOCK_STREAM)
sock.settimeout(5)
try: sock.bind(addr)
except:
    print "ERROR: Couldn't connect on port %d" % port
    sys.exit(2)

# listen for a connection
sock.listen(5)

# create the watchdog settings
wdog = WatchdogThread()
wdog.start()

timeout = config['Configuration']['ServerTimeout'] * 60
running = True
reset_time = time.time()

# get the logger
logger = getLogger()

# start the monitoring
from Ganga.Core import monitoring_component
monitoring_component.enableMonitoring()

# main loop
while True:
    
    # accept connections
    while running:
        
        conn_failed = False
        
        try:
            conn, addr = sock.accept()
        except:
            conn_failed = True
            pass
        
        if not conn_failed:
            break
        
        # check for kill file or timeout
        if os.path.exists( os.path.join(config["Configuration"]["gangadir"], "server", "server.kill") ) or (time.time() - reset_time) > timeout:
            if not conn_failed:
                conn.close()

            sock.close()

            # close watchdog
            wdog.running = False
            wdog.join()

            os.system("rm -f %s" % os.path.join(config["Configuration"]["gangadir"], "server", "server.kill"))            
            os.system("rm -f %s" % os.path.join(config["Configuration"]["gangadir"], "server", "server.info"))
                    
            sys.exit(0)
            
    # grab the data
    data = conn.recv(1024)
    while data.find("###ENDCMD###") == -1 and data.find("###STOP###") == -1:
        data += conn.recv(1024)

    # update the reset time
    reset_time = time.time()
    
    # check for graceful shutdown
    if not data or data == "###STOP###":
        # exit and then close
        conn.send("###STOPPED###")
        conn.close()
        break
    else:
        # set the stdout/stderr
        codeOut = StringIO.StringIO()


        tstamp = time.time()
        logger.info("###Server command started %d ###" % tstamp)
        
        sys.stdout = codeOut
        sys.stderr = codeOut

        try:
            exec data
        except:
            print "Error while executing script: %s" % formatTraceback()
            
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

        logger.info("###Server command completed###")

        # grab the logger results
        stdout = open( os.path.join(config["Configuration"]["gangadir"], "server", "server-%s.stderr" % os.uname()[1]) ).read()
        start_pos = stdout.find("###Server command started %d ###" % tstamp)
        end_pos = stdout.find("###Server command completed###", start_pos)
        msg = stdout[start_pos + len("###Server command started %d ###" % tstamp):end_pos]
        
        # send everything
        conn.send(msg + codeOut.getvalue() + "###ENDMSG###")
        conn.close()
        
# close the connection
sock.close()

os.system("rm -f %s" % os.path.join(config["Configuration"]["gangadir"], "server", "server.kill"))            
os.system("rm -f %s" % os.path.join(config["Configuration"]["gangadir"], "server", "server.info"))


# close watchdog
wdog.running = False
wdog.join()
