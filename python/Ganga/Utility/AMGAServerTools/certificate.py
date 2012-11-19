#!/bin/env python
#----------------------------------------------------------------------------
# Name:         certificate.py
# Purpose:      utility to get grid certificate content
#
# Author:       Alexander Soroko
#
# Created:      21/03/2006
#----------------------------------------------------------------------------

import sys
import os
import re
from Commands   import submitCmd

DEBUG = False
#DEBUG = True

## Methods
#---------------------------------------------------------------------------
def getCertificateSubject(certPath = '', certFile = ''):
    """Returns certificate subject.
    If certPath is provided, than it looks for the certificate in that path.
    If certFile is provided, than takes it instead of default name 'usercert.pem'.
    """
    subject = ''

    if not certFile:
        certFile = "usercert.pem"
    if certPath:
        certFile = os.path.join(certPath, certFile)
        
    # create command line:
    cmd = 'openssl x509 -subject -in %s -nameopt oneline -noout' %certFile

    # submit command
    executed, output = submitCmd(cmd)

    if DEBUG:
        print executed, output

    # check output
    if executed:
        if len(output) > 0:
            subject =  output[0]
            r = re.match('(subject\s*= )(.*)', subject)
            if r:
                subject = r.group(2)
                return subject
    return subject

#--------------------------------------------------------------------------------------
def  getGridProxyPath():
    """Returns path to the grid proxy certificate of current user.
    A valid proxy must be created independently."""
    
    proxyPath = ""
    if os.environ.has_key( "X509_USER_PROXY" ):
        proxyPath = os.environ[ "X509_USER_PROXY" ]
    else:
        proxyPath = "".join( [ "/tmp/x509up_u", str( os.getuid() ) ] )

    if not os.path.exists( proxyPath ):
        proxyPath = ""

    return proxyPath

################################################################################
usage = """
certificate.py <option>/ <certPath> <sertFile>
option = -i -- interactive mode
option = -h -- show command usage
certPath    -- path to the certificate file
certFile    -- name of the certificate file
"""



if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == '-i':
            while 1:
                certPath = raw_input('Enter path to the certificate (If different from current directory)--->')
                certFile = raw_input('Enter name of the certificate file (If different from "usercert.pem") --->')
                print getCertificateSubject(certPath, certFile)
                quit = raw_input('Enter "Q" to exit--->')
                if quit.lower() == 'q':
                    break
        elif sys.argv[1] == '-h':
            print usage
        else:
            if len(sys.argv) > 2:
                print getCertificateSubject(sys.argv[1], sys.argv[2])
            else:
                print getCertificateSubject(sys.argv[1])
    else:
        print getCertificateSubject()
    
