###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: __init__.py,v 1.1 2009-05-10 16:42:03 karl Exp $
###############################################################################
# File: PDF/__init__.py
# Author: K. Harrison
# Created: 070122

"""Package initialisation file"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "9 May 2009"
__version__ = "1.0"

from PDF import PDF
from PDF_LCG import PDF_LCG
from PDF_Local import PDF_Local
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add( "PDF", "Interactive", PDF_Local )
allHandlers.add( "PDF", "Local", PDF_Local )

allHandlers.add( "PDF", "LCG", PDF_LCG )
