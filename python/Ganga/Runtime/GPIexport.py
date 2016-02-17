##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: GPIexport.py,v 1.1 2008-07-17 16:41:00 moscicki Exp $
##########################################################################

""" Utility for exporting symbols to GPI.
"""
from __future__ import absolute_import


# all public GPI names will be exported here
import Ganga.GPI

from .gangadoc import adddoc

from Ganga.GPIDev.Base.Proxy import isType, addProxy, getProxyClass
from Ganga.GPIDev.Base.Objects import GangaObject
from inspect import isclass

def _setInInterface(interfaces, name, _object):
    for interface in interfaces:
        setattr(interface, name, _object)

def exportToGPI(name, _object, doc_section, docstring=None, extra_interface=None):
    '''
    Make object available publicly as "name" in Ganga.GPI module. Add automatic documentation to gangadoc system.
    "doc_section" specifies how the object should be documented.
    If docstring is specified then use it to document the object (only use for "Objects" section). Otherwise use __doc__ (via pydoc utilities).
    FIXME: if you try to export the object instance, you should import it with fully qualified path, e.g.
     import X.Y.Z
     X.Y.Z.object = object
     exportToGPI("obj",X.Y.Z.object,"Objects")

    It has been observed that doing exportToGPI("obj",object,"Objects") may not work. To be understood.
    '''

    interfaces = [Ganga.GPI]

    if extra_interface is not None:
        interfaces.append(extra_interface)

    if isType(_object, GangaObject):
        exposed_object = addProxy(_object)
    elif isclass(_object) and issubclass(_object, GangaObject):
        exposed_object = getProxyClass(_object)
    else:
        exposed_object = _object

    _setInInterface(interfaces, name, exposed_object)

    adddoc(name, getattr(Ganga.GPI, name), doc_section, docstring)

def _importFromGPI(name):
    '''
    Method to import back an object from the GPI which is NOT wrapped in a proxy!
    '''
    return stripProxy(getattr(Ganga.GPI, name))

    # print 'EXPORTED',name,object
#
#
# $Log: not supported by cvs2svn $
# Revision 1.3  2007/07/10 13:08:32  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.2  2006/06/21 11:42:30  moscicki
# comments
#
# Revision 1.1  2005/08/24 15:24:11  moscicki
# added docstrings for GPI objects and an interactive ganga help system based on pydoc
#
#
#
