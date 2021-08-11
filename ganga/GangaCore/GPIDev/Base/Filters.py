# Customization of GPI component object assignment: Component Filters
#
# Example of usage:
#
#  j.f = 'myfile'  <=>  j.f = File('myfile')
#  j.application = 'DaVinci' <=> DaVinci()
#
#  j = Job()
#  j2 = Job(j)  <=> copy constructor
#
#
#
# Semantics:
#  gpi_proxy.x = v  --> f = select_filter(category_of(x)); f(v,schema_item_of(x))
#  gpi_proxy.y = [v1,v2] --> f = select_filter(category_of(x)); [f(v,schema_item_of(y)) for v in [v1,v2]]
#  x = X(y,...) --> f = select_filter(X._category); f(y,None)
#
# Component filters are applied *only* to component properties and they
# are applied *before* attribute filters.

# Component filter has the following signature:
#     filter(val,item) --> return a GangaObject instance or None if no conversion performed
#
#     item is None if the filter is called outside of the attribute assignment context
#
# If conversion takes place filter MUST return an object which is an
# instance (derived) of GangaObject.

# Void filter does nothing. This is the default filter if no other default
# has been defined.

from GangaCore.GPIDev.Base.Proxy import isType, getName
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Config.Config import ConfigError

# test configuration properties
config = getConfig('GPIComponentFilters')

def void_filter(val, item):
    return None


class _ComponentFilterManager(object):

    __slots__ = ('default', '_dict')

    def __init__(self):
        # for each category there may be multiple filters registered, the one used being defined
        # in the configuration file in [GPIComponentFilters]
        # e.g: {'datasets':{'lhcbdatasets':lhcbFilter,
        # 'testdatasets':testFilter}...}
        self._dict = {}
        self.default = None

    def __setitem__(self, category, _filter):

        if category not in self._dict:
            self._dict[category] = {}

        # the filter can be registered as a tuple: ('filtername',filterfunction)
        # or just as a function in which case the function name is used as an
        # alias
        if isType(_filter, tuple) and len(_filter) >= 2:
            filtername = _filter[0]
            filterfunc = _filter[1]
        else:
            try:
                filtername = getName(_filter)
                filterfunc = _filter
            except AttributeError as e:
                raise ValueError(
                    'FilterManager: Invalid component filter %s.' % _filter)

        if filtername in self._dict[category]:
            raise ValueError('FilterManager: %s component filter already exists for %s category ' % (filtername, category))

        if category not in config.options:
            config.addOption(category, "", "")
        config.overrideDefaultValue(category, filtername)
        self._dict[category][filtername] = filterfunc

    def setDefault(self, _filter):
        if self.default:
            raise ValueError('FilterManager: default filter already exists')

        self.default = _filter

    def __getitem__(self, category):
        try:
            filters = self._dict[category]
        except KeyError:
            # no filters registered for this category
            if self.default:
                return self.default
            return void_filter

        try:
            filtername = config[category]
            return filters[filtername]
        except ConfigError:
            # if we have only one filter registered for this category we use it
            if len(filters) == 1:
                return list(filters.values())[0]
            else:  # ambiguity
                raise ValueError('FilterManager: Multiple filters detected for %s category: %s, '
                                 'but no one has be set as default in [GPIComponentFilters] section of the configuration file'
                                 % (category, str(list(filters.keys()))))
        except KeyError:
            # wrong filter name in configuration for this category
            raise ValueError('FilterManager: %s filter is not registered for %s category.'
                             'Check your [GPIComponentFilters] section of the configuration file'
                             % (filtername, category))


# all filters register here...
allComponentFilters = _ComponentFilterManager()
