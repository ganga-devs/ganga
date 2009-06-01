#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import sys
import pickle
import traceback
from socket import *

# get the port,etc. from the command line
port = int(sys.argv[1])
end_data_str = sys.argv[2]
dirac_cmds = sys.argv[3]
server_id = int(sys.argv[4])

# try and connect to this port
addr = ('localhost',port)
sock = socket(AF_INET,SOCK_STREAM)
try: sock.bind(addr)
except:
    sys.exit(2)
sock.listen(1) # listen for 1 connection
conn, addr = sock.accept()

# import DIRAC API
from DIRAC.Interfaces.API.Dirac import *
from DIRAC.Interfaces.API.Job import *
from DIRAC.LHCbSystem.Utilities.AncestorFiles import getAncestorFiles
from DIRAC.LHCbSystem.Client.LHCbJob import LHCbJob
from DIRAC import gLogger
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

execfile(dirac_cmds)

def get_server_id():
    global server_id
    return server_id

def execute_dirac_command(cmd):
    '''Executes a command (handles the decoration).'''
    command = cmd.replace(end_data_str,'') 
    presult = ''
    result = None
    try:
        exec(command)
        presult = pickle.dumps(result)
    except:
        presult = pickle.dumps(traceback.format_exc()+'###TRACEBACK###')    
    return presult + end_data_str

# listen for commands
cmd = ''
while True:
    data = conn.recv(1024)
    if not data:
        # exit and then close
        break
    else:        
        cmd += data
        if(cmd.find(end_data_str) >= 0):
            result = execute_dirac_command(cmd)
            conn.send(result)
            cmd = ''

# close the connection
conn.close()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
