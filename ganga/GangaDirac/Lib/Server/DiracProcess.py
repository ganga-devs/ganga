#!/usr/bin/env python
import sys
import os
import errno
import socket
import traceback
HOST = 'localhost'  # Standard loopback interface address (localhost)
PORT = int(sys.argv[1])        # Port to listen on
rand_hash = raw_input() 
import time
#We have to define an output function as a placeholder here.
def output(data):
    pass

#A function to shutdown an existing processes
def closeSocket():
    sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sc.connect((HOST, PORT))
    sc.sendall(b'close server')
    sc.close()
end_trans = '###END-TRANS###'
#This is a wrapper for the client sockets
class socketWrapper(object):

    def __init__(self, skt):
        self._socket = skt

    def read(self):
        cmd = ''
        while end_trans not in cmd:
            data = self._socket.recv(1024)
            if not data:
                cmd = '###BROKEN###'
                break
            cmd += data
        #Check the random string is in the cmd so we know it came from a trusted source.
        if not rand_hash in cmd:
            return 'close-connection'
        if cmd == '###BROKEN###':
            return ''
        return_string = cmd.replace(rand_hash, '')
        return return_string.replace(end_trans, '')

#Start the socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1024)
#Set 30 minute timeout
s.settimeout(1800)
while True:
    try:
        conn, addr = s.accept()
        sock = socketWrapper(conn)
        cmd = sock.read()
        #Here we define the output method to just send the output of the diracCommand wrapper.
        def output(data):
            conn.sendall(repr(data))

        if cmd=='close-connection':
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()
            continue

        if cmd=='close-server':
            conn.shutdown(socket.SHUT_RDWR)
            conn.close()
            break
        try:
            print(eval(cmd))
        except:
            try:
                exec(cmd)
            except:
                print("Exception raised executing command (cmd) '%s'\n" % cmd)
                print(traceback.format_exc())

        conn.sendall('###END-TRANS###')
    #Catch the timeout and exit
    except socket.timeout:
        break

s.shutdown(socket.SHUT_RDWR)
s.close()
