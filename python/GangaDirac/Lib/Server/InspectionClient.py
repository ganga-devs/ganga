from __future__ import print_function

import sys
import socket
import collections
from pprint import pformat
from GangaCore.Utility.ColourText import getColour
SocketAddress = collections.namedtuple('SocketAddress', ['address', 'port'])


def runClient():
    end_trans = '###END-TRANS###'
    socket_addr = SocketAddress(address='localhost', port=5000)
    while 1:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(socket_addr)
        cmd = raw_input(getColour('fg.blue') + 'diracAPI_env > ' + getColour('fg.normal'))
        client_socket.sendall(cmd + end_trans)
        if (cmd == 'q' or cmd == 'Q' or cmd == 'exit' or cmd == 'exit()'):
            client_socket.close()
            break

        out = ''
        while end_trans not in out:
            data = client_socket.recv(1024)
            if not data:
                out = '###BROKEN###'
                break
            out += data

        if out != '###BROKEN###':
            out = out.replace(end_trans, '')
            if out == '':
                print('')
            else:
                try:
                    print(getColour('fg.red') + 'diracAPI_env > ' +
                          getColour('fg.normal') + pformat(eval(out)) + '\n')
                except:
                    print(
                        getColour('fg.red') + 'diracAPI_env > ' + getColour('fg.normal') + out)
        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()


if __name__ == '__main__':
    runClient()
