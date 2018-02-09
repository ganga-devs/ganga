from GangaCore.Utility.logging import getLogger
from GangaCore.Core.exceptions import GangaValueError
logger = getLogger()


class PluginManagerError(GangaValueError):

    def __init__(self, x):
        super(PluginManagerError, self).__init__(self, x)

# Simple Ganga Plugin Mechanism
#
# Any object may be registered (added) in the plugin manager provided that
# you are able to specify the name and the category to which it belongs.
#
# If you do not use category all plugins are registered in a flat list. Otherwise
# there is a list of names for each category seaprately.


class PluginManager(object):

    __slots__ = ('all_dict', 'first', '_prev_found')

    def __init__(self):
        self.all_dict = {}
        self.first = {}
        self._prev_found = {}

    def find(self, category, name):
        """
        Return a plugin added with 'name' in the given 'category'.
        If 'name' is None then the default plugin in the category is returned.
        Typically the default plugin is the first added.
        If plugin not found raise PluginManagerError.
        """
        #logger.debug( "Attempting to Find Plugin: %s" % name )
        #import traceback
        # traceback.print_stack()

        # Simple attempt to pre-load and cache Plugin lookups
        key = str(category) + "_" + str(name)
        if key in self._prev_found:
            return self._prev_found[key]

        try:
            if name is not None:
                if category in self.first:
                    ## This is expected to work and is quite verbose when debugging turned on
                    #logger.debug("Returning based upon Category and Name")
                    #logger.debug("name: %s cat: %s" % (str(name), str(category)))
                    if name in self.all_dict[category]:
                        self._prev_found[key] = self.all_dict[category][name]
                        return self.all_dict[category][name]

            if (name is None) and category is not None:
                if (category in self.first):
                    ## This is expected to work and is quite verbose when debugging turned on
                    #logger.debug("Returning based upon Category ONLY")
                    #logger.debug("name: %s cat: %s" % (str(name), str(category)))
                    self._prev_found[key] = self.first[category]
                    return self.first[category]

            elif (name is not None) and (category is not None):
                for category_i in self.all_dict:
                    for this_name in self.all_dict[category_i]:
                        if name == this_name:
                            message1 = "Category of %s, has likely changed between ganga versions!" % name
                            message2 = "Category Requested: %s,   Category in which plugin was found: %s" % (category, category_i)
                            message3 = "Attempting to use new category %s to load a stored object, this may fail!" % category_i
                            logger.debug(message1)
                            logger.debug(message2)
                            logger.debug(message3)
                            self._prev_found[key] = self.all_dict[category_i][name]
                            return self.all_dict[category_i][name]

        except KeyError:
            logger.debug("KeyError from Config system!")
        except:
            logger.error("Some Other unexpected ERROR!")
            raise

        if name is None:
            s = "cannot find default plugin for category " + category
        else:
            s = "cannot find '%s' in a category '%s', or elsewhere" % (name, category)

        if name is None and category is None:
            s = "Serious Plugin Error has occured"

        logger.debug(s)
        raise PluginManagerError(s)

    def add(self, pluginobj, category, name):
        """ Add a pluginobj to the plugin manager with the name and the category labels.
        The first plugin is default unless changed explicitly.
        """
        cat = self.all_dict.setdefault(category, {})
        self.first.setdefault(category, pluginobj)
        cat[name] = pluginobj
        logger.debug('adding plugin %s (category "%s") ' % (name, category))

    def setDefault(self, category, name):
        """ Make the plugin 'name' be default in a given 'category'.
        You must first add() the plugin object before calling this method. Otherwise
        PluginManagerError is raised.
        """
        assert(not name is None)
        pluginobj = self.find(category, name)
        self.first[category] = pluginobj

    def allCategories(self):
        return self.all_dict

    def allClasses(self, category):
        cat = self.all_dict.get(category)
        if cat:
            return cat
        else:
            return {}

allPlugins = PluginManager()

