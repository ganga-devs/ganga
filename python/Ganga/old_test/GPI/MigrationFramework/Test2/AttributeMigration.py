###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AttributeMigration.py,v 1.1 2008-07-17 16:41:13 moscicki Exp $
###############################################################################

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject


class TestAttribute(GangaObject):

    """The test attribute for the migration framework"""

    _schema = Schema(Version(2, 0), {
        'options': FileItem(doc='Attribute options')
    })

    _category = 'testAttributes'
    _name = 'TestAttribute'
    _exportmethods = []


######### migration ######################################################
    def getMigrationClass(cls, version):
        """This class method returns a (stub) class compatible with the schema <version>.
        Alternatively, it may return a (stub) class with a schema more recent than schema <version>,
        but in this case the returned class must have "getMigrationClass" and "getMigrationObject"
        methods implemented, so that a chain of convertions can be applied."""
        return TestAttribute12
    getMigrationClass = classmethod(getMigrationClass)

    def getMigrationObject(cls, obj):
        """This method takes as input an object of the class returned by the "getMigrationClass" method,
        performs object transformation and returns migrated object of this class (cls)."""
        # check that obj has shema supported for migration
        version = obj._schema.version
        old_cls = cls.getMigrationClass(version)
        if old_cls:  # obj can be converted
            converted_obj = cls()
            for attr, item in converted_obj._schema.allItems():
                # specific convertion stuff
                if attr == 'options':
                    converted_obj.options.name = obj.options
                else:
                    setattr(converted_obj, attr, getattr(obj, attr))
            return converted_obj
    getMigrationObject = classmethod(getMigrationObject)
######## migration #######################################################


###### migration ##############################################################

class TestAttribute12(TestAttribute):

    """The test application for the migration framework"""

    _schema = Schema(Version(1, 0), {'options': SimpleItem(defvalue='', doc='Attribute options')
                                     })

    _category = 'attributes_converters'
    _name = 'TestAttribute12'
