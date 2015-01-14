import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

class PluginManagerError(ValueError):
    def __init__(self,x): ValueError.__init__(self,x)
    
# Simple Ganga Plugin Mechanism
#
# Any object may be registered (added) in the plugin manager provided that
# you are able to specify the name and the category to which it belongs.
# 
# If you do not use category all plugins are registered in a flat list. Otherwise
# there is a list of names for each category seaprately.

class PluginManager(object):
    def __init__(self):
        self.all_dict = {}
        self.first = {}

    def find(self, category, name):
        """
        Return a plugin added with 'name' in the given 'category'.
        If 'name' is None then the default plugin in the category is returned.
        Typically the default plugin is the first added.
        If plugin not found raise PluginManagerError.
        """
        logger.debug( "Attempting to Find Plugin: %s" % name )
        try:
            if name is not None:
                if category in self.first:
                    logger.debug( "Returning based upon Category and Name" )
                    return self.all_dict[category][name]

            if (name is None) and (category in self.first):
                logger.debug( "Returning based upon Category ONLY" )
                return self.first[category]
            elif not category in self.first:
                for category_i in self.all_dict:
                    if name in self.all_dict[category_i]:
                        message1 = "Category of %s, has likely changed between ganga versions!" % name
                        message2 = "Category Requested: %s,   Category in which plugin was found: %s" % ( category, category_i )
                        message3 = "Attempting to use new category %s to load a stored object, this may fail!" % category_i
                        logger.debug( message1 )
                        logger.debug( message2 )
                        logger.debug( message3 )
                        return self.all_dict[category_i][name]
            else:
                return self.all_dict[category][name]

        except KeyError:
            if name is None:
                s = "cannot find default plugin for category "+category
            else:
                s = "cannot find '%s' in a category '%s', or elsewhere" %(name, category)

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

