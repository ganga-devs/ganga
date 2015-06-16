# Ganga Project. http://cern.ch/ganga
#
# $Id: associations.py,v 1.2 2009-03-10 12:19:20 moscicki Exp $
##########################################################################
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

__author__ = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__ = "23 November 2006"
__version__ = "1.0"

from Ganga.Utility.Config import makeConfig
config = makeConfig(
    "File_Associations", 'Default associations between file types and file-viewing commands. The name identifies the extension and the value the commans. New extensions can be added. A single & after the command indicates that the process will be started in the background. A && after the command indicates that a new terminal will be opened and the command executed in that terminal.', is_open=True)

config.addOption("newterm_command", "xterm",
                 'Command for opening a new terminal (xterm, gnome-terminal, ...')
config.addOption("newterm_exeopt", "-e",
                 'Option to give to a new terminal to tell it to execute a command.')
config.addOption(
    "listing_command", "ls -ltr", 'Command for listing the content of a directory')
config.addOption('fallback_command', 'less',
                 'Default command to use if there is no association with the file type')
config.addOption('htm', 'firefox &', 'Command for viewing html files.')
config.addOption('html', 'firefox &', 'Command for viewing html files.')
config.addOption('root', 'root.exe &&', 'Command for opening ROOT files.')
config.addOption('tar', 'file-roller &', 'Command for opening tar files.')
config.addOption('tgz', 'file-roller &', 'Command for opening tar files.')
