#!/usr/bin/env python
#
# $Id: Octopus.py,v 1.1 2008-07-17 16:40:59 moscicki Exp $
#

import socket
import random
import sys
import time

DEBUG = True


class ProtocolException(Exception):

    """ Raised when the protocol fails.
    """

    def __init__(self, errorCode, msg):
        self.errorCode = errorCode
        self.msg = msg

    def __str__(self):
        return repr(self.errorCode) + ' - ' + repr(self.msg)


class Octopus:

    def __init__(self, host, port=8882):
        self.connected = False
        self.host = host
        self.port = port
        self.buffer = ''
        self.eotFound = False

    def connect(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        TCP_NODELAY = 1
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if DEBUG:
            print 'Connecting to ', self.host, self.port
        self.s.connect((self.host, self.port))
        self.connected = True
        self.buffer = ''

    def open(self, channel, requestNew):
        if DEBUG:
            print 'Opening channel ', channel, ' create is ', requestNew
        if not self.connected:
            try:
                self.connect()
            except socket.error as e:
                raise ProtocolException(-1, "Could not connect to server "
                                        + e.__str__())
        try:
            if requestNew == 1:
                self.s.send('create %s\n\n' % channel)
            elif requestNew == -1:
                self.s.send('join %s \n\n' % channel)
            else:
                self.s.send('channel %s\n\n' % channel)
            response = ''
            while response.find("\n\n") < 0:
                buf = self.s.recv(1024)  # OK from server
                if not buf:
                    raise ProtocolException(-4, "Server disconnect")
                response = response + buf
#                if len(response) > 500:
#                    raise ProtocolException(-2, "Server header too long")
            pos = response.find("ERROR ")
            if pos > -1:
                err = int(response.split(' ')[1])
                raise ProtocolException(err, "Server error: " + response[pos:])
            pos = response.find("OK\n\n")
            if pos < 0:
                raise ProtocolException(-3, "Illegal server header")
            self.buffer = response[pos + 4:]
            self.s.setblocking(0)
        except socket.error as e:
            raise ProtocolException(-1, "Could not connect to server "
                                    + e.__str__())

    def close(self, sendEOT=True):
        if sendEOT:
            self.send('\004')
        self.s.close()

    def create(self, channel=0):
        if channel == 0:
            channel = randint(1, sys.maxsize)
        return self.open(channel, 1)

    def join(self, channel=0):
        if channel == 0:
            channel = randint(1, sys.maxsize)
        return self.open(channel, 0)

    def send(self, message):
        if DEBUG:
            print 'Sending ', message
        try:
            self.s.send(message)
        except socket.error as e:
            raise ProtocolException(-5, "Send failed: " + e.__str__())

    def read(self, length=-1):
        if len(self.buffer) > 0:
            result = self.buffer
            self.buffer = ''
        else:
            result = ''

        if length == -1:
            self.s.setblocking(0)
            result = result + self.s.recv(1024)
            pos = result.find('\004')
            if pos > -1:
                self.eotFound = True
            return result
        self.s.setblocking(1)
        result = result + self.s.recv(length)
        pos = result.find('\004')
        if pos > -1:
            self.eotFound = True

        return result
