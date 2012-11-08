#!/usr/bin/env python
#
# $Id: oreader.py,v 1.1 2008-07-17 16:40:59 moscicki Exp $
#


from Octopus import *
import getopt, sys, errno
import time
import os
import sys

try:
    s = os.environ['GANGA_OCTOPUS_SERVER']
except KeyError:
    s = 'localhost'

try:
    p = os.environ['GANGA_OCTOPUS_PORT']
except KeyError:
    p = 8882                                        

octopus = Octopus(s, p)
try:
    channel = long(sys.argv[1])
except IndexError:
    print >> sys.stderr, 'Usage: oreader <channel>'
    print >> sys.stderr, '       channel: The channel number to join on the server'
    print >> sys.stderr, 'The server to connect to is defined by the GANGA_OCTOPUS_SERVER and GANGA_OCTOPUS_PORT environment variables'
    sys.exit(5)

octopus.join(channel)

while not octopus.eotFound:
    try:
        data = octopus.read()
    except socket.error, e:
        if e[0] != errno.EAGAIN: 
            raise socket.error(e)
        data = ''
    if len(data) <=0 :
        time.sleep(0.1)
    else:
        sys.stdout.write(data)
