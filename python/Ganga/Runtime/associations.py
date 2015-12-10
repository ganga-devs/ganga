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


def load_associations():
    pass
