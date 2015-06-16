#!/bin/env python
#----------------------------------------------------------------------------
# Name:         directoryManagement.py
# Purpose:      utility to manage user directories and permissions
#
# Author:       Alexander Soroko
#
# Created:      21/03/2006
#----------------------------------------------------------------------------

import sys
import os
import re
from certificate import getGridProxyPath
from mdclient import MDClient
from mdinterface import CommandException

DEBUG = False
#DEBUG = True

#---------------------------------------------------------------------------


class Collections:

    """Represents interface for manipulating collections (directories)"""

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
    def createDir(self, dir):
        """Creates the directory dir if it does not yet exist but parent dir
        already exist"""
        self._client.createDir(dir)

    #-----------------------------------------------------------------------
    def listDir(self, dir):
        """Returns names of all subdirectories in the directory dir"""
        res = []
        self._client.listEntries(dir)
        while not self._client.eot():
            d, t = self._client.getEntry()
            if DEBUG:
                print d, t[0]
            if t[0] == 'collection':
                res.append(d)
        return res

    #-----------------------------------------------------------------------
    def statDir(self, dir):
        """Returns owner and owner-permissions for the directory dir"""
        res = []
        cmd = 'stat ' + dir
        self._client.execute(cmd)
        while not self._client.eot():
            row = self._client.fetchRow()
            if DEBUG:
                print row
            res.append(row)
        return res

    #-----------------------------------------------------------------------
    def removeDir(self, dir):
        """Removes all directories matching path. Directories are only deleted
        if they are empty and they have no attributes defined"""
        self._client.removeDir(dir)

    #-----------------------------------------------------------------------
    def pwd(self):
        """Returns the current directory"""
        return self._client.pwd()

    #-----------------------------------------------------------------------
    def cd(self, dir):
        """Changes the current directory to the given directory"""
        self._client.cd(dir)

    #-----------------------------------------------------------------------
    def chown(self, dir, new_owner):
        """Changes the owner of the directory"""
        cmd = 'chown ' + dir + ' ' + new_owner
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def chmod(self, dir, new_permissions):
        """Changes owner permidssions for the directory.
        The format of new_permissions is rwx, where "-" signs can be
        substituted for the letters if certain priviledges have to be
        ommitted"""
        cmd = 'chmod ' + dir + ' ' + new_permissions
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def aclAdd(self, dir, group, rights):
        """Adds group rights to the dir ACL.
        The format of the group user:groupname.
        The format of rights is rwx"""
        cmd = 'acl_add ' + dir + ' ' + group + ' ' + rights
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def aclRemove(self, dir, group):
        """Removes group from the dir ACL.
        The format of the group user:groupname"""
        cmd = 'acl_remove ' + dir + ' ' + group
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def aclShow(self, dir):
        """Shows the dir ACL"""
        res = []
        cmd = 'acl_show ' + dir
        self._client.execute(cmd)
        while not self._client.eot():
            row = self._client.fetchRow()
            if DEBUG:
                print row
            res.append(row.split(' '))
        return res


##########################################################################
usage = """
"""
