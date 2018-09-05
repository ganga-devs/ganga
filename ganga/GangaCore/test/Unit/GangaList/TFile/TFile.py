##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TFile.py,v 1.1 2008-07-17 16:41:11 moscicki Exp $
##########################################################################
from GangaCore.GPIDev.Lib.File.File import File
from GangaCore.GPIDev.Schema.Schema import SimpleItem
from GangaCore.GPIDev.Base.Objects import GangaObject

from GangaCore.GPIDev.Base.Proxy import isType


class TFile(GangaObject):

    """Test File object with well known equality properties -i.e. Does not reply on proxy!"""

    _category = 'files'
    _exportmethods = ['__eq__', '__cmp__', '__hash__', '__iadd__', '__imul__']
    _name = 'TFile'
    _schema = File._schema.inherit_copy()
    _schema.datadict['added'] = SimpleItem(defvalue=False)
    _schema.datadict['multiplied'] = SimpleItem(defvalue=False)

    def __eq__(self, other):
        if not isType(other, TFile):
            return False
        return self.name == other.name and self.subdir == other.subdir

    def __cmp__(self, other):
        """A hacky but correct cmp function."""
        self_comb = self.name + self.subdir
        other_comb = other.name + other.subdir
        return cmp(self_comb, other_comb)

    def __hash__(self):
        return self.name.__hash__() + self.subdir.__hash__()

    def __iadd__(self, other):
        self.added = True
        return self

    def __imul__(self, other):
        self.multiplied = True
        return self
