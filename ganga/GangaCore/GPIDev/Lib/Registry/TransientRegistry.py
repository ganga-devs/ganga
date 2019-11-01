import fnmatch
import copy
from GangaCore.Core.GangaRepository.Registry import Registry, RegistryKeyError
from GangaCore.GPIDev.Lib.Registry.RegistrySlice import RegistrySlice
from GangaCore.GPIDev.Lib.Registry.RegistrySliceProxy import RegistrySliceProxy, _wrap
from GangaCore.GPIDev.Base.Proxy import stripProxy
from GangaCore.Utility.logging import getLogger
logger = getLogger()


class TransientRegistry(Registry):

    def __init__(self, name, filebase, doc, file_ext='tpl', pickle_files=False):

        super(TransientRegistry, self).__init__(name,
                                                doc)
        self.type = "ImmutableTransient"
        self.location = filebase
        self.file_ext = file_ext
        self.pickle_files = pickle_files
        self._needs_metadata = False

        self.stored_slice = TransientRegistrySlice(self.name)
        self.stored_slice.objects = self
        self.stored_slice.add = self.add
        self.stored_proxy = TransientRegistrySliceProxy(self.stored_proxy)

    # def startup(self):
        # Note call the base class setup as dont want
        # metadata which JobRegistry forces on us
        #super(TransientRegistry, self).startup()
        # Registry.startup(self)

    def _getName(self, obj):
        return obj.name

    def add(self, obj, name):
        """
        Add an object to the registry
        """
        o = copy.deepcopy(stripProxy(obj))
        #o = stripProxy(obj)
        o.name = name
        #o._registry = self
        #o._registry_id = -1
        # print stripProxy(obj), o
        super(TransientRegistry, self)._add(o)

    def getSlice(self):
        return self.stored_slice

    def getProxy(self):
        return self.stored_proxy


class TransientRegistrySlice(RegistrySlice):

    def __init__(self, name):
        super(TransientRegistrySlice, self).__init__(
            name, display_prefix="box")
        from GangaCore.Utility.ColourText import Foreground, Background, Effects
        fg = Foreground()
        fx = Effects()
        bg = Background()
        self.fx = fx
        self.name = 'box'  # needed to ensure that select works properly
        self.status_colours = {'default': fx.normal,
                               'JobTemplate': fg.orange,
                               'Task': fg.green,
                               'Job': fg.blue}
        self._display_columns_functions["id"] = lambda obj: obj.id
        self._display_columns_functions["type"] = lambda obj: obj._name
        self._display_columns_functions["name"] = lambda obj: obj.name
        self._proxyClass = TransientRegistrySliceProxy

    def _getColour(self, obj):
        try:
            return self.status_colours.get(getName(obj), self.fx.normal)
        except Exception as err:
            return self.status_colours['default']

    def __call__(self, id):
        """
        Retrieve an object by id.
        """
        if isinstance(id, str):
            if id.isdigit():
                id = int(id)
            else:
                matches = [
                    o for o in self.objects if fnmatch.fnmatch(o.name, id)]
                if len(matches) > 1:
                    logger.error(
                        'Multiple Matches: Wildcards are allowed for ease of matching, however')
                    logger.error(
                        '                  to keep a uniform response only one item may be matched.')
                    logger.error(
                        '                  If you wanted a slice, please use the select method')
                    raise RegistryKeyError(
                        "Multiple matches for id='%s':%s" % (id, str([x.name for x in matches])))
                if len(matches) < 1:
                    return _wrap(TransientRegistrySlice(self.name))
                return matches[0]
        try:
            return self.objects[id]
        except KeyError:
            raise RegistryKeyError('Object id=%d not found' % id)


class TransientRegistrySliceProxy(RegistrySliceProxy):

    def __init__(self, impl):
        super(TransientRegistrySliceProxy, self).__init__(impl)

    def __call__(self, x):
        """
        Access individual object. Examples:
        """
        return _wrap(stripProxy(self).__call__(x))

    def __getitem__(self, x):
        """
        Get an item by positional index. Examples:
        """
        return _wrap(stripProxy(self).__getitem__(x))

