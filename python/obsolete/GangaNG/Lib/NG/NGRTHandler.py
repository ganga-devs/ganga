###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: NGRTHandler.py,v 1.1 2008-07-17 16:41:29 moscicki Exp $
###############################################################################
#
# NG backend
#
# Maintained by the Oslo group (B. Samset, K. Pajchel)
#
# Date:   January 2007



import os, socket, pwd, commands, re

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Credentials import GridProxy


class NGRTHandler(IRuntimeHandler):

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaNG.Lib.NG import NGJobConfig
        
        return NGJobConfig(app.exe,app._getParent().inputsandbox,app.args,app._getParent().outputsandbox,app.env)

                                    
allHandlers.add('Executable', 'NG', NGRTHandler)

# $Log: not supported by cvs2svn $
# Revision 1.1  2007/02/28 13:45:11  bsamset
# Initial relase of GangaNG
#
