################################################################################# Ganga Project. http://cern.ch/ganga
#
# $Id: associations.py,v 1.1 2008-07-17 16:41:00 moscicki Exp $
#################################################################################
# File: associations.py
# Author: K. Harrison
# Created: 061123
#
"""
Module defining default associations
between file types and file-viewing commands.
Values are stored in a configuration object,
and modifications/additions can be made
in the [File_Associations] section of a
configuration file.
"""
                                                                                
__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "23 November 2006"
__version__ = "1.0"

from Ganga.Utility.Config import makeConfig
config = makeConfig( "File_Associations",'default associations between file types and file-viewing commands.' , is_open=True)

config.addOption("newterm_command","xterm",'FIXME')
config.addOption("newterm_exeopt","-e",'FIXME')
config.addOption("listing_command","ls -lhtr",'FIXME')
config.addOption('fallback_command','less &&','FIXME')
config.addOption('html','mozilla &','FIXME')
config.addOption('root','root.exe &&','FIXME')


## config.setDefaultOptions( {
##    "newterm_command" : "xterm",
##    "newterm_exeopt" : "-e",
##    "listing_command" : "ls -lhtr",
##    "fallback_command" : "less &&",
##    "html" : "mozilla &",
##    "root" : "root.exe &&"
##    } )
