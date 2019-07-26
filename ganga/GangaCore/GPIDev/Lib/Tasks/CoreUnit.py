from GangaCore.GPIDev.Base.Proxy import addProxy
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.GPIDev.Lib.Tasks.IUnit import IUnit
from GangaCore.GPIDev.Lib.Tasks.common import makeRegisteredJob
import copy


class CoreUnit(IUnit):
    _schema = Schema(Version(1, 0), dict(list(IUnit._schema.datadict.items())))

    _category = 'units'
    _name = 'CoreUnit'
    _exportmethods = IUnit._exportmethods + []

    def __init__(self):
        super(CoreUnit, self).__init__()

    def createNewJob(self):
        """Create any jobs required for this unit"""
        j = makeRegisteredJob()

        j.backend = self._getParent().backend.clone()

        # copy form ourselves or the parent transform depending on what's
        # specified
        fields = ['application', 'splitter', 'inputfiles',
                  'inputdata', 'inputsandbox', 'outputfiles', 'postprocessors']

        for f in fields:

            if (f == "postprocessors" and len(getattr(self, f).process_objects) > 0):
                j.postprocessors = copy.deepcopy(addProxy(self).postprocessors)
            elif (f != "postprocessors" and getattr(self, f)):
                setattr(j, f, copy.deepcopy(getattr(self, f)))
            elif (f == "postprocessors" and len(getattr(self._getParent(), f).process_objects) > 0):
                j.postprocessors = copy.deepcopy(
                    addProxy(self._getParent()).postprocessors)
            elif (f != "postprocessors" and getattr(self._getParent(), f)):
                setattr(j, f, copy.deepcopy(getattr(self._getParent(), f)))

        return j
