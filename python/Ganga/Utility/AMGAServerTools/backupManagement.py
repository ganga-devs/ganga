#!/bin/env python
#----------------------------------------------------------------------------
# Name:         backupManagement.py
# Purpose:      utility to back up user directories, users and groups
#
# Author:       Alexander Soroko
#
# Created:      01/05/2006
#----------------------------------------------------------------------------

from __future__ import absolute_import
import sys
import os
import re
from .certificate import getGridProxyPath
from .mdclient import MDClient
from .mdinterface import CommandException

#DEBUG = False
DEBUG = True

#---------------------------------------------------------------------------


class BackUp:

    """Represents interface to back up user directories, users and groups"""

    def __init__(self,
                 host='gangamd.cern.ch',
                 port=8822,
                 login='root',
                 password='',
                 keepalive=False,
                 reqSSL=True,
                 **kwds):

        self._client = MDClient(host=host,
                                port=port,
                                login=login,
                                password=password,
                                keepalive=keepalive)

        if reqSSL:
            fn = getGridProxyPath()
            key = kwds.get('key')
            if not key:
                key = fn
            cert = kwds.get('cert')
            if not cert:
                cert = fn

            self._client.requireSSL(key, cert)
            self._client.connect()

    #-----------------------------------------------------------------------
    def dump(self, dir):
        """Returns list of commands needed to resore directory dir"""
        res = []
        cmd = 'dump ' + dir
        self._client.execute(cmd)
        while not self._client.eot():
            row = self._client.fetchRow()
            if DEBUG:
                print row
            res.append(row)
        return res

    #-----------------------------------------------------------------------
    def dumpToFile(self, dir, filename):
        """Dumps directory dir to a file filename"""
        res = self.dump(dir)
        ff = open(filename, 'w')
        try:
            for cmd in res:
                cmd = cmd + "\n"
                ff.write(cmd)
        finally:
            ff.close()

        if DEBUG:
            ff = open(filename, 'r')
            try:
                cmds = ff.readlines()
            finally:
                ff.close()
            for cmd in cmds:
                print cmd[:-1]

    #-----------------------------------------------------------------------
    def restoreFromFile(self, dir, filename):
        """Restores content of a directory dir from a file"""
        ff = open(filename, 'r')
        try:
            cmds = ff.readlines()
        finally:
            ff.close()
        pwd = self._client.pwd()
        self._client.cd(dir)
        try:
            for cmd in cmds:
                try:
                    cmd = cmd[:-1]  # remove newline character
                    if DEBUG:
                        print "executing command:\n" + cmd + "\n"
                    self._client.execute(cmd)
                except Exception as e:
                    print str(e)
        finally:
            self._client.cd(pwd)


##########################################################################
usage = """
"""
