###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: __init__.py,v 1.1 2008-10-04 17:42:38 karl Exp $
###############################################################################
# File: Classify/__init__.py
# Author: K. Harrison
# Created: 070122

"""Package initialisation file"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "22 February 2007"
__version__ = "1.0"

from Classify import Classify
from ClassifyLCG import ClassifyLCG
from ClassifyLocal import ClassifyLocal
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add( "Classify", "Interactive", ClassifyLocal )
allHandlers.add( "Classify", "Local", ClassifyLocal )

allHandlers.add( "Classify", "LCG", ClassifyLCG )
