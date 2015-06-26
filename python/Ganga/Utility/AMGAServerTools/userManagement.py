#!/bin/env python
#----------------------------------------------------------------------------
# Name:         userManagement.py
# Purpose:      utility to manage users using the database backend
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


class UserDB:

    """Represents db interface for user management"""

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
    def userList(self):
        """Lists all users known to the authentication subsustem"""
        res = []
        cmd = 'user_list'
        self._client.execute(cmd)
        while not self._client.eot():
            row = self._client.fetchRow()
            if DEBUG:
                print row
            res.append(row)
        return res

    #-----------------------------------------------------------------------
    def userListCred(self, user):
        """Lists the credentials with which the user can be authenticated"""
        res = []
        cmd = 'user_listcred ' + user
        self._client.execute(cmd)
        while not self._client.eot():
            row = self._client.fetchRow()
            if DEBUG:
                print row
            res.append(row)
        return res

    #-----------------------------------------------------------------------
    def userCreate(self, user, password=''):
        """Creates a new user and assigns a password if given."""
        cmd = 'user_create ' + user
        if password:
            cmd += ' ' + password
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def userRemove(self, user):
        """Deletes a user"""
        cmd = 'user_remove ' + user
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def userPasswordChange(self, user, password):
        """Changes the password of a user"""
        cmd = 'user_password_change ' + user + ' ' + password
        self._client.execute(cmd)

    #-----------------------------------------------------------------------
    def userSubjectAdd(self, user, subject):
        """Adds a certificate identified by its subject line to be used to
        authenticate a user"""
        cmd = 'user_subject_add ' + user + ' ' + '\'' + subject + '\''
        self._client.execute(cmd)


##########################################################################
usage = """
"""
