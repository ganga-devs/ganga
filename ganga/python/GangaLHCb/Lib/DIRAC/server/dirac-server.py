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
import DIRAC
#from DIRAC.Interfaces.API.Dirac import *
#from DIRAC.Interfaces.API.Job import *
#from DIRAC.LHCbSystem.Utilities.AncestorFiles import getAncestorFiles
#from DIRAC.LHCbSystem.Client.LHCbJob import LHCbJob
#from DIRAC import gLogger
#from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

execfile(dirac_cmds)

def get_server_id():
    global server_id
    return server_id

counter = 0

def execute_dirac_command(cmd):
    '''Executes a command (handles the decoration).'''
    command = cmd.replace(end_data_str,'')
    global counter
    #if counter > 0:
    #    return pickle.dumps(command) + end_data_str
    #else: counter += 1
    presult = ''
    result = None
    try:
        exec(command)
        #if counter > 0: result = 'OK'
        #else: counter += 1
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
            conn.sendall('###RECV-DATA###')
            result = execute_dirac_command(cmd)
            conn.sendall(result)
            cmd = ''

# close the connection
conn.close()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
