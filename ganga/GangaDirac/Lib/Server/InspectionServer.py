from __future__ import print_function

# For running inspections of the environment
import sys
import socket
import traceback
import collections
SocketAddress = collections.namedtuple('SocketAddress', ['address', 'port'])

end_trans = '###END-TRANS###'
socket_addr = SocketAddress(address='localhost', port=5000)
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind(socket_addr)
server_socket.listen(5)
server_stdout = sys.stdout


class socketWrapper(object):

    def __init__(self, skt):
        self._socket = skt
        sys.stdout = self
        sys.stderr = self

    def write(self, stuff):
        self._socket.sendall(stuff)

    def read(self):
        cmd = ''
        while end_trans not in cmd:
            data = self._socket.recv(1024)
            if not data:
                cmd = '###BROKEN###'
                break
            cmd += data
        if cmd == '###BROKEN###':
            return ''
        return cmd.replace(end_trans, '')

# print "TCPServer Waiting for client on port 5000"


while True:
    client_socket, address = server_socket.accept()
    print("Connection request from ", SocketAddress(*address), file=server_stdout)

    sock = socketWrapper(client_socket)

    cmd = sock.read()
    if (cmd == 'q' or cmd == 'Q' or cmd == 'exit' or cmd == 'exit()'):
        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()
        break

    print("Executing command '%s'" % cmd, file=server_stdout)
    try:
        print(eval(cmd))
    except:
        try:
            exec(cmd)
        except:
            print("Exception raised executing command (cmd) '%s'\n" % cmd)
            print(traceback.format_exc())
    client_socket.sendall(end_trans)
    client_socket.shutdown(socket.SHUT_RDWR)
    client_socket.close()
server_socket.shutdown(socket.SHUT_RDWR)
server_socket.close()

