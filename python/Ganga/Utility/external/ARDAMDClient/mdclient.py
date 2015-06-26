##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: mdclient.py,v 1.1 2008-07-17 16:41:02 moscicki Exp $
##########################################################################
import socket
import mdinterface
import time
from mdinterface import CommandException, MDInterface

DEBUG = False

try:
    import tlslite
    USE_TLSLITE = True
    from tlslite.api import *
    if DEBUG:
        print "Using tlslite"
except ImportError as e:
    USE_TLSLITE = False


class MDClient(MDInterface):

    def __init__(self, host, port,
                 login='anonymous', password='',
                 keepalive=True):

        self.connected = 0
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.keepalive = keepalive
        self.buffer = ''
        self.reqSSL = False
        self.sslSock = 0
        self.sslOptions = None
        self.sessionID = 0
        self.session = None
        self.greetings = ''
        self.protocolVersion = 0
        self.currentCommand = ""
        self.siteCache = {}
        self.idCache = {}

    def requireSSL(self, key=None, cert=None):
        self.reqSSL = True
        self.keyFile = key
        self.certFile = cert
        if USE_TLSLITE:
            self.sslOptions = tlslite.HandshakeSettings.HandshakeSettings
            self.sslOptions.minVersion = (3, 0)
            self.sslOptions.maxVersion = (3, 1)

    def __doSSLHandshake(self, session=None):
        if USE_TLSLITE:
            self.sslSock = TLSConnection(self.s)
            if session:
                if DEBUG:
                    print "Doing SSL resuming session using TLSLite"
                self.sslSock.handshakeClientCert(session=session)
                return
            if DEBUG:
                print "Doing SSL handshake using TLSLite"
            cert = None
            key = None
            if self.certFile:
                s = open(self.certFile).read()
                x509 = X509()
                x509.parse(s)
                cert = X509CertChain([x509])
            if self.keyFile:
                s = open(self.keyFile).read()
                key = parsePEMKey(s, private=True)
            self.sslSock.handshakeClientCert(cert, key,
                                             self.session, None, None, False)
            self.sslSock.closeSocket = False
        else:
            if DEBUG:
                print "Doing SSL handshake using builtin SSL"
            self.sslSock = socket.ssl(self.s, self.keyFile, self.certFile)

    def connect(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        TCP_NODELAY = 1
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.s.connect((self.host, self.port))

        self.greetings = ""
        # Expect 3 lines of greetings: server version, protocol, auth-methods
        while self.greetings.count('\n') < 3:
            line = self.s.recv(1024)
            if not line:
                raise CommandException(-1, "Cannot connect")
            self.greetings += line

        pos = self.greetings.find('\nProtocol ')
        if pos > -1:
            self.protocolVersion = int(self.greetings[pos + 10])

        if self.sessionID:
            if DEBUG:
                print "Trying to resume session ", self.sessionID
            # Do reconnect
            if self.reqSSL:
                self.s.send('resumeSSL%u\n\n' % self.sessionID)
                line = self.s.recv(1024)  # OK from server
                self.__doSSLHandshake(self.session)
            else:
                self.s.send('resume%d\n\n' % self.sessionID)
                self.connected = True
                self.buffer = ''
                self.attr = 0
                return 0
        else:
            if self.reqSSL:
                self.s.send('ssl\n\n')
                line = self.s.recv(1024)  # OK from server
                self.__doSSLHandshake()
            else:
                self.s.send('plain\n\n')
                line = self.s.recv(1024)  # OK from server

        # Send login information if not doing reconnect
        context = '0 ' + self.login
        if len(self.password):
            context = context + '\n5 ' + self.password
        context = context + '\n\n'

        if not self.sessionID:
            if self.sslSock:
                self.sslSock.write(context)
            else:
                self.s.send(context)

        self.connected = True
        self.buffer = ''
        self.attr = 0

    def disconnect(self, saveSession=False):
        if saveSession and self.sslSock:
            if USE_TLSLITE:
                self.session = self.sslSock.session
            else:
                self.session = None
        if self.connected:
            if self.sslSock:
                if USE_TLSLITE:
                    self.sslSock.close()
                else:
                    del self.sslSock
            self.s.close()
            self.connected = False

    def __sendCommand(self, command):
        if DEBUG:
            print "Sending > ", command, "<"
        if self.sslSock:
            self.sslSock.write(command + '\n')
        else:
            self.s.send(command + '\n')

    def __encodeCommand(self, command):
        command.replace('//', '////')
        command.replace('\n', '//n')
        return command

    def execute(self, command):
        """ Returns silently if the command executes successfully,
            throws an exception otherwise
        """
        command = self.__encodeCommand(command)
        self.currentCommand = command
        self.buffer = ""
        if not self.keepalive:
            self.disconnect()
        if not self.connected:
            self.connect()
        self.__sendCommand(command)
        return self.retrieveResult()

    def retrieveResult(self):
        self.EOT = 0
        self.buffer = ""
        line = self.__fetchRow()
        if line == None:
            raise IOError("Server sent empty response")
        pos = line.find(' ')
        msg = ""
        if(pos > -1):
            retValue, msg = int(line[:pos]), line[pos + 1:]
        else:
            retValue = int(line)
        if retValue != 0:
            # The command did not execute properly. Clear
            # the input buffer and raise an exception
            while not self.EOT:
                if self.__fetchData() < 0:
                    break
            self.buffer = ""
            msg = msg + ". Command was: " + self.currentCommand
            raise CommandException(retValue, msg)

    def executeNoWait(self, command):
        """ Returns silently if the command executes,
            throws an exception if an error occours immediately
            Does not wait for any return condition of the remote
            command
        """
        command = self.__encodeCommand(command)
        self.currentCommand = command
        self.buffer = ""
        if not self.keepalive:
            self.disconnect()
        if not self.connected:
            self.connect()
        self.__sendCommand(command)
        if(self.__dataArrived()):
            return self.retrieveResult()

    def fetchRow(self):
        return self.__fetchRow()

    def __fetchRow(self):
        pos = self.buffer.find('\n')
        if pos > -1:
            line = self.buffer[:pos]
            self.buffer = self.buffer[pos + 1:]
            if not len(self.buffer) and not self.EOT:
                if self.__fetchData() < 0:
                    return None
            return line
        if self.EOT:
            return None
        if self.__fetchData() <= 0:
            return None
        return self.__fetchRow()

    def __fetchData(self):
        while True:
            # Look for end of transmission
            pos = self.buffer.find('\004')
            if pos > -1:
                # Need to find two EOT chars if protocol > 1
                while self.protocolVersion > 1 and self.buffer.find('\004', pos + 1) < 0:
                    if self.sslSock:
                        line = self.sslSock.read(1024)
                    else:
                        line = self.s.recv(1024)
                    if not line:
                        break
                    self.buffer += line
                break
            # Look for newline
            pos = self.buffer.find('\n')
            if pos > -1 and pos < len(self.buffer) - 1:
                break

            # Do read to find newline
            if self.sslSock:
                line = self.sslSock.read(1024)
            else:
                line = self.s.recv(1024)
            if not line:
                break
            self.buffer += line
        pos = self.buffer.find('\004')
        if pos > -1:
            self.sessionID = 0
            if pos < len(self.buffer) - 8 and self.buffer[pos + 1:pos + 8] == 'session':
                pos2 = self.buffer.find('\004', pos + 1)
                self.sessionID = long(self.buffer[pos + 8:pos2])
                self.disconnect(True)
            if DEBUG:
                print "Session ID", self.sessionID
            self.buffer = self.buffer[:pos]
            self.EOT = 1
        if not line:
            return -1
        return len(line)

    def __dataArrived(self):
        gotSomething = 1
        self.s.setblocking(0)
        try:
            self.s.recv(1, MSG_PEEK)
        except Exception as e:
            gotSomething = 0
        self.s.setblocking(1)
        return gotSomething

    def __quoteValue(self, value):
        if isinstance(value, type(str())):
            value = value.replace('\'', '\\\'')
            value = "'" + value + "'"
        return value

    def eot(self):
        if len(self.buffer):
            return 0
        if self.EOT:
            return 1
        if self.__fetchData() <= 0:
            return 1
        return not len(self.buffer)

    def getattr(self, file, attributes):
        command = 'getattr ' + file
        for i in attributes:
            command += ' ' + i
        self.nattrs = len(attributes)
        self.execute(command)

    def getEntry(self):
        file = self.__fetchRow()
        attributes = []
        for i in range(0, self.nattrs):
            attribute = self.__fetchRow()
            attributes.append(attribute)
        return file, attributes

    def setAttr(self, file, keys, values):
        command = 'setattr ' + file
        for i in range(len(keys)):
            command += ' ' + keys[i]
            values[i] = self.__quoteValue(values[i])
            command += ' ' + str(values[i])
        self.execute(command)

    def addEntry(self, file, keys, values):
        command = 'addentry ' + file
        for i in range(len(keys)):
            command += ' ' + keys[i]
            values[i] = self.__quoteValue(str(values[i]))
            command += ' ' + values[i]
        self.execute(command)

    def addEntries(self, entries):
        command = 'addentries'
        for e in entries:
            command += ' ' + e
        self.execute(command)

    def addAttr(self, file, name, t):
        command = 'addattr ' + file + ' ' + name + ' ' + t
        self.execute(command)

    def removeAttr(self, file, name):
        command = 'removeattr ' + file + ' ' + name
        self.execute(command)

    def clearAttr(self, file, name):
        command = 'clearattr ' + file + ' ' + name
        self.execute(command)

    def listEntries(self, pattern):
        command = 'dir ' + pattern
        self.execute(command)
        self.nattrs = 1

    def pwd(self):
        self.execute('pwd')
        return self.__fetchRow()

    def listAttr(self, file):
        command = 'listattr ' + file
        self.execute(command)
        attributes = []
        types = []
        while not self.eot():
            attribute = self.__fetchRow()
            attributes.append(attribute)
            t = self.__fetchRow()
            types.append(t)
        return attributes, types

    def createDir(self, dir):
        command = 'createdir ' + dir
        self.execute(command)

    def createPlainDir(self, dir):
        command = 'createdir ' + dir + ' plain'
        self.execute(command)

    def removeDir(self, dir):
        command = 'rmdir ' + dir
        self.execute(command)

    def rm(self, path):
        command = 'rm ' + path
        self.execute(command)

    def find(self, pattern, query):
        command = 'find '
        command += ' ' + pattern
        command += ' ' + self.__quoteValue(query)
        self.nattrs = 1
        self.execute(command)

    def selectAttr(self, attributes, query):
        command = 'selectattr '
        for i in attributes:
            command += ' ' + i
        self.nattrs = len(attributes)
        command += ' ' + self.__quoteValue(query)
        self.execute(command)

    def getSelectAttrEntry(self):
        attributes = []
        for i in range(0, self.nattrs):
            attribute = self.__fetchRow()
            attributes.append(attribute)
        return attributes

    def updateAttr(self, pattern, updateExpr, condition):
        command = 'updateattr ' + pattern
        for i in updateExpr:
            var, exp = self.splitUpdateClause(i)
            command += ' ' + var + ' ' + self.__quoteValue(exp)
        command += ' ' + self.__quoteValue(condition)
        self.execute(command)

    def update(self, pattern, updateExpr, condition):
        command = 'update ' + pattern
        for i in updateExpr:
            var, exp = self.splitUpdateClause(i)
            command += ' ' + var + ' ' + self.__quoteValue(exp)
        command += ' ' + self.__quoteValue(condition)
        self.execute(command)

    def upload(self, collection, attributes):
        command = 'upload ' + collection
        for i in attributes:
            command += ' ' + i
        self.nattrs = len(attributes)
        self.executeNoWait(command)

    def put(self, file, values):
        command = 'put ' + file
        if(len(values) != self.nattrs):
            raise CommandException(3, "Illegal command")
        for i in values:
            command += ' ' + i
        self.executeNoWait(command)

    def abort(self):
        command = 'abort'
        self.execute(command)

    def commit(self):
        command = 'commit'
        self.execute(command)

    def sequenceCreate(self, name, directory, increment=1, start=1):
        command = 'sequence_create ' + name + " " + directory + " "
        command += str(increment) + " " + str(start)
        self.execute(command)

    def sequenceNext(self, name):
        command = 'sequence_next ' + name
        self.execute(command)
        return self.__fetchRow()

    def sequenceRemove(self, name):
        command = 'sequence_remove ' + name
        self.execute(command)

    def cd(self, dir):
        command = 'cd ' + dir
        self.execute(command)

    def constraintAddNotNull(self, directory, attribute, name):
        command = 'constraint_add_not_null ' + \
            directory + " " + attribute + " " + name
        self.execute(command)

    def constraintAddUnique(self, directory, attribute, name):
        command = 'constraint_add_unique ' + \
            directory + " " + attribute + " " + name
        self.execute(command)

    def constraintAddReference(self, directory, attribute, reffered_attr, name):
        command = 'constraint_add_reference ' + \
            directory + ' ' + attribute + ' ' + reffered_attr
        command = command + ' ' + name
        self.execute(command)

    def constraintAddCheck(self, directory, check, name):
        command = 'constraint_add_check ' + directory + \
            ' ' + self.__quoteValue(check) + ' ' + name
        self.execute(command)

    def constraintDrop(self, directory, name):
        command = 'constraint_drop ' + directory + ' ' + name
        self.execute(command)

    def constraintList(self, directory):
        command = 'constraint_list ' + directory
        self.execute(command)
        constraints = []
        while not self.eot():
            constraint = self.__fetchRow()
            constraints.append(constraint)
        return constraints

    def transaction(self):
        command = 'transaction'
        self.execute(command)

    def remoteExecute(self, remoteCommand):
        command = 'execute ' + remoteCommand
        self.execute(command)
        result = ''
        while not self.eot():
            result = result + self.__fetchRow() + '\n'
        return result

    def siteAdd(self, site, server=''):
        command = 'site_add ' + site
        if len(server):
            command = command + ' ' + server
        self.execute(command)

    def siteRemove(self, site):
        command = 'site_remove ' + name
        self.execute(command)

    def replicaRegister(self, guidSiteList, create=True):
        command = 'replica_register'
        if create:
            command = command + ' -c'
        for g in guidSiteList:
            (guid, site) = g
            command += ' '
            if site in self.siteCache:
                command = command + guid + ' ' + str(self.siteCache[site])
            else:
                command = command + guid + ' ' + site
        self.execute(command)

    def replicaUnregister(self, guid, site, delete=False):
        command = 'replica_unregister '
        if delete:
            command = command + '-d '
        command = command + guid + ' ' + site
        self.execute(command)

    def replicaAdd(self, guid):
        command = 'replica_add '

    def siteList(self):
        command = 'site_list'
        self.execute(command)
        self.siteCache = {}
        self.idCache = {}
        while not self.eot():
            l = self.__fetchRow()
            (id, name, config) = l.split()
            self.siteCache[name] = int(id)
            self.idCache[int(id)] = name
        return self.siteCache

    def replicaList(self, files, resolveSites=True, isLFN=False):
        if resolveSites and not len(self.idCache):
            self.siteList()
        command = 'replica_list'
        if isLFN:
            command += ' -l'
        for f in files:
            command += ' ' + f
        self.execute(command)
        res = {}
        while not self.eot():
            l = self.__fetchRow()
            try:
                (file, s) = l.split(' ', 1)
            except ValueError:
                file = l
                s = ''
            sites = map(int, s.split())
            if resolveSites:
                sites = map(self.idCache.get, sites)
            res[file] = sites
        return res
