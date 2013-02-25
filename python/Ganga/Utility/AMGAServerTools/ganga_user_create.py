#!/bin/env python
#----------------------------------------------------------------------------
# Name:         ganga_user_create.py
# Purpose:      creates ganga user account and
#               sets up home directory in the repository
#
# Author:       Alexander Soroko
#
# Created:      21/04/2006
#----------------------------------------------------------------------------

import sys
import os
import re
from certificate import getCertificateSubject
from userManagement import UserDB
from directoryManagement import Collections


#_defaultMinVersion = "2.2"
#_defaultMinHexVersion = 0x20200f0
_defaultPlatform = 'slc3_gcc323'
_defaultExternalHome = "/afs/cern.ch/sw/ganga/external"

DEBUG = False
#DEBUG = True

#---------------------------------------------------------------------------
def translateSubject(subject):
    ss = subject.split('/')
    if len(ss) < 2:
        return subject
    else:
        ss = ', '.join(ss[1:])
        ss = re.sub(r'=(?!\s+)', '= ', ss)
        ss = re.sub(r'(?<!\s)=', ' =', ss)
        return ss

#---------------------------------------------------------------------------
def create(user,
           cert_subject = '',
           cert_path = '',
           cert_file = '',
           home_dir = '',
           **kwds):

    # create user 
    udb = UserDB(**kwds)
    try:
        udb.userCreate(user, password = 'ganga')
    except Exception, e:
        print str(e)
        return
    try:
        if not cert_subject:
            if cert_path == '':
                cert_path = '/afs/cern.ch/user/'+ user[0] + '/' + user + '/.globus'
            cert_subject = getCertificateSubject(cert_path, cert_file)
        if cert_subject:
            udb.userSubjectAdd(user, translateSubject(cert_subject))
        else:
            print "WARNING: Can't get certificate subject. \n\
            Certificate subject was't mapped to the user " + user
    except Exception, e:
        print str(e)   

    # create user directory
    cls = Collections(**kwds)
    if home_dir == '':
        home_dir = '/users/' + user
    try:
        cls.createDir(home_dir)
    except Exception, e:
        print str(e)    
        return
    try:
        cls.chown(home_dir, user)
        cls.chmod(home_dir, 'rwx')
    except Exception, e:
        print str(e)    
        return    

#---------------------------------------------------------------------------
def main():
        
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("--cert-subject", dest = "cert_subject",
                      help = "subject of user certificate")    
    parser.add_option("--cert-file", dest = "cert_file",
                      help = "name of user certificate file")
    parser.add_option("--cert-path", dest = "cert_path",
                      help = "path where to look for the user certificate")
    parser.add_option("--home-dir", dest = "home_dir",
                      help = "name of user home directory in the repository")
    parser.add_option("--host", dest = "host",
                      help = "url of the repository server")
    parser.add_option("--port", dest = "port", type = "int",
                      help = "port number")
    parser.add_option("--login", dest = "login", 
                      help = "login name for the administrator")
    parser.add_option("--password", dest = "password", 
                      help = "administrator's password")        

    (options, args) = parser.parse_args()

    if len(args) > 0:
        user = args[0]
    else:
        print "No user name was given. Exiting..."
        return
    
    kwargs = {}
    for o in ['cert_subject','cert_file','cert_path','home_dir','host','port','login','password']:
        v = getattr(options, o)
        if v:
            kwargs[o] = v

    create(user, **kwargs)
    
    
################################################################################
usage = """
"""

if __name__ == '__main__':
    main()
