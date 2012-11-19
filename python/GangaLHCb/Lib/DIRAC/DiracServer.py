#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
import pickle
import time
import re
import random
import inspect
import tempfile
from subprocess import *
from socket import *
from Ganga.GPIDev.Base.Objects import GangaObject
import Ganga.Utility.logging
from Ganga.Core import GangaException
from Ganga.GPIDev.Credentials import getCredential
import Ganga.Utility.Config

configLHCb = Ganga.Utility.Config.getConfig('LHCb')
configDirac = Ganga.Utility.Config.getConfig('DIRAC')
logger = Ganga.Utility.logging.getLogger() 

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def build_command_string(cmd,end_data_str):
    return '''
%s
%s''' % (cmd, end_data_str)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracServer:
    '''Handles running and commincating w/ the DIRAC server application.'''
    
    port_blacklist = None
    dirac_env = None

    def __init__(self):
        self.end_data_str = '###END-DATA-TRANS###'
        self.port_min = 49512 # Mininum dynamic/private port number.
        self.port_max = 65535 # Maximum port number.
        self.server = None
        self.socket = None
        self.port = None        
        self.path = os.path.dirname(inspect.getsourcefile(DiracServer))
        logger.debug('Set DiracServer.path = %s' % self.path)
        self.server_id = random.randrange(0,os.sys.maxint)
        self.show_stdout = None
        self.proxy = getCredential('GridProxy', '')

    def _getPortBlacklist(self):
        '''Obtains a list of ports used by system services. '''
        blacklist = []
        output = Popen(["cat", "/etc/services"],stdout=PIPE).communicate()

        if output[0]: 
            regex = re.compile('\d+\/\w\wp')
            service_ports = regex.findall(output[0])
            for port in service_ports:
                port_number = int(port.split('/')[0])
                if port_number not in blacklist:
                    if port_number >= self.port_min:
                        blacklist.append(port_number)

        DiracServer.port_blacklist = blacklist
        logger.debug('DiracServer.port_blacklist = %s' % str(blacklist))

    def _getDiracEnv(self):
        '''Gets the DIRAC environment.'''
        if not os.environ.has_key("GANGADIRACENVIRONMENT"):
            raise GangaException('DIRAC env cache file does not exist.')
        cache_file = os.environ["GANGADIRACENVIRONMENT"]
        if not os.path.exists(cache_file):
            raise GangaException('DIRAC env cache file does not exist.')
        env = {}
        count = 0 
        f = open(cache_file)
        for line in f.readlines():
            if line.find('DIRAC') >= 0: count += 1
            varval = line.strip().split('=')
            env[varval[0]] = ''.join(varval[1:])
        f.close()
        if count == 0:
            msg = 'Error creating DIRAC environment cache file.'
            raise GangaException(msg)
        DiracServer.dirac_env = env

    def _run(self,port):
        '''Run the server.'''
        server_script = '%s/server/dirac-server.py' % self.path
        dirac_cmds = '%s/server/DiracCommands.py' % self.path
        cmd = 'python %s %d \'%s\' %s %d' % (server_script,port,
                                             self.end_data_str,
                                             dirac_cmds,self.server_id)
        stdout = open('/dev/null','w')
        if configDirac['ShowDIRACstdout']: stdout = None
           
        self.server = Popen([cmd],shell=True,env=DiracServer.dirac_env,
                            stdout=stdout,stderr=STDOUT)
        self.show_stdout = configDirac['ShowDIRACstdout']
        time.sleep(float(configDirac['StartUpWaitTime']))
        rc = self.server.poll()
        return rc

    def isActive(self):
        '''Checks if the server is still running.'''
        if self.server is None: return False
        if self.server.poll() is None: return True
        else: return False

    def read_stdout(self):
        '''Returns stdout/stderr from server (not implemented yet)'''
        return 'NOT AVAILABLE'

    def connect(self):
        '''Run the server and connect it and the client to the same port.'''
        if not DiracServer.port_blacklist: self._getPortBlacklist()
        if not DiracServer.dirac_env: self._getDiracEnv()
        self.socket = None
        self.port = None
        ports_tried = 0
        while ports_tried < 100:
            port = random.randrange(self.port_min,self.port_max+1)
            if port in DiracServer.port_blacklist: continue
            ports_tried += 1
            logger.debug('Attempting to connect to port %d...' % port)
            rc = self._run(port)
            if rc is not None:
                os.system('kill -9 %d >& /dev/null' % self.server.pid)
                continue            
            self.socket = socket(AF_INET,SOCK_STREAM)
            try:
                self.socket.connect(('localhost',port))
                # authenticate
                cmd = 'result = get_server_id()'
                self._send(cmd)
                result = self._recv(cmd)
                if result != self.server_id:
                    self.socket.close()
                    continue
                self.port = port                
                break
            except:
                logger.debug('Connection failed.')
                pass

        if self.port is None:
            stdout = self.read_stdout()
            msg = 'Failed to open server/client connection [stdout = %s]' \
                  % stdout 
            raise GangaException(msg)
        logger.debug('Connected to port %d' % self.port)
        return self.port

    def _send(self,cmd):
        '''Sends the command to the server (decorates it appropriately).'''
        command = build_command_string(cmd,self.end_data_str)
        self.socket.sendall(command)
        if not self.isActive():
            stdout = self.read_stdout()
            msg = 'Server died after attempting to execute command: %s ' \
                  '[stdout = %s]' % (cmd,stdout)
            raise GangaException(msg)
        return command

    def _recv(self,cmd):
        '''Recieves the result from the server (waits to get it all).'''
        presult = ''
        recv_completed = False
        while self.isActive():
            try:
                data = self.socket.recv(1024)
                #if(data.find('###RECV-DATA###') >= 0): data = ''
            except error, e:
                msg = 'Timeout attempting to receive result of command: %s' \
                      ' (w/ error: %s)'% (cmd,e)
                logger.error(msg)
                self.disconnect()
                raise GangaException('Timeout in Dirac socket connection.')
                
            presult += data
            if(presult.find(self.end_data_str) >= 0):
                start_data = presult.find('###RECV-DATA###')
                if(start_data >= 0):
                    presult = presult[start_data + len('###RECV-DATA###'):]
                recv_completed = True
                presult = presult.replace(self.end_data_str,'')
                result = pickle.loads(presult)
                break
                            
        if not recv_completed:
            if not self.isActive():
                stdout = self.read_stdout()
                msg = 'Server died while attempting to receive result of ' \
                      'command %s [stdout = %s]' % (cmd,stdout)
            else:
                msg = 'An unknown error occured while trying to receive a '\
                      'command result from the Dirac server.  Please contact '\
                      'the Ganga team!'
            raise GangaException(msg) 
        return result

    def execute(self,cmd,timeout=None):
        '''Sends the command to the server then waits for it to return the
        result.'''
        if not self.proxy.isValid(): 
            self.proxy.create()
            if not self.proxy.isValid():
                msg = 'Can not execute DIRAC API code w/o a valid grid proxy.'
                raise GangaException(msg)
        if not self.isActive(): self.connect()
        elif self.show_stdout is not configDirac['ShowDIRACstdout']:
            self.disconnect()
            self.connect()
        self.socket.settimeout(float(configDirac['Timeout']))
        if timeout is not None:
            self.socket.settimeout(timeout)
            logger.debug('Server timeout set to %d s.' % timeout)
        logger.debug('Sending %s to Dirac server.' % cmd)
        self._send(cmd)
        result = self._recv(cmd)
        if type(result) == type('') and result.find('###TRACEBACK###') >= 0:
            print result.replace('###TRACEBACK###','')
            raise GangaException('Exception executing DIRAC API code.')
        logger.debug('Received %s from Dirac server.' % result)
        self.socket.settimeout(float(configDirac['Timeout']))
        return result
    
    def disconnect(self):
        '''Closes the connection and kills the server.'''
        self.socket.close()
        # wait for it to close (should take less than a second)
        num_polls = 0
        while self.isActive():
            num_polls += 1
            if num_polls >= 100: break
            time.sleep(0.1)
        try:
            os.system('kill -9 %d >& /dev/null' % self.server.pid)
        except: pass
        return not self.isActive()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
