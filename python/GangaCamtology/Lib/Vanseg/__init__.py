###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: __init__.py,v 1.1 2008-10-04 17:42:39 karl Exp $
###############################################################################
# File: Vanseg/__init__.py
# Author: K. Harrison
# Created: 070122

"""Package initialisation file"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "22 February 2007"
__version__ = "1.0"

from Vanseg import Vanseg
from VansegLCG import VansegLCG
from VansegLocal import VansegLocal
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add( "Vanseg", "Interactive", VansegLocal )
allHandlers.add( "Vanseg", "Local", VansegLocal )

allHandlers.add( "Vanseg", "LCG", VansegLCG )
