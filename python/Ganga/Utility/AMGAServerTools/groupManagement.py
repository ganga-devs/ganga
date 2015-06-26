#!/bin/env python
#----------------------------------------------------------------------------
# Name:         groupManagement.py
# Purpose:      utility to manage user groups
#
# Author:       Alexander Soroko
#
# Created:      21/03/2006
#----------------------------------------------------------------------------

from __future__ import absolute_import
import sys
import os
import re
from .certificate import getGridProxyPath
from .mdclient import MDClient
from .mdinterface import CommandException

DEBUG = False
#DEBUG = True

#---------------------------------------------------------------------------


class Groups:

    """Represents interface for manipulating user groups"""

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
    def grpCreate(self, groupname):
        """Creates a new group with name groupname.
        It is not possible to create groups beloning to others."""
        res = []
        cmd = 'grp_create ' + groupname
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def grpDelete(self, groupname):
        """Deletes a group with name groupname (user:groupname).
        Only root can delete groups of other users"""
        cmd = 'grp_delete ' + groupname
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def grpShow(self, groupname):
        """Shows all the members belonging to group gropname."""
        res = []
        cmd = 'grp_show ' + groupname
        self._client.execute(cmd)
        while not self._client.eot():
            row = self._client.fetchRow()
            if DEBUG:
                print row
            res.append(row)
        return res

    #-----------------------------------------------------------------------
    def grpAddUser(self, groupname, user):
        """Adds a user to a group.
        Only owners of a group or root can change group membership"""
        cmd = 'grp_adduser ' + groupname + ' ' + user
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def grpRemoveUser(self, groupname, user):
        """Removes a user from a group.
        Only owners of a group or root can change group membership"""
        cmd = 'grp_removeuser ' + groupname + ' ' + user
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def grpMember(self, user=''):
        """Shows to which groups a user belongs"""
        res = []
        cmd = 'grp_member'
        if user:
            cmd += ' ' + user
        self._client.execute(cmd)
        while not self._client.eot():
            row = self._client.fetchRow()
            if DEBUG:
                print row
            res.append(row)
        return res

    #-----------------------------------------------------------------------
    def grpList(self, user=''):
        """Shows the groups owned by user, by default the current user"""
        res = []
        cmd = 'grp_list'
        if user:
            cmd += ' ' + user
        self._client.execute(cmd)
        while not self._client.eot():
            row = self._client.fetchRow()
            if DEBUG:
                print row
            res.append(row)
        return res


##########################################################################
usage = """
"""
