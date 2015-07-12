###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ApplicationMigration.py,v 1.1 2008-07-17 16:41:13 moscicki Exp $
###############################################################################

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.IApplication import IApplication


class TestApplicationMigration(IApplication):

    """The test application for the migration framework"""

    _schema = Schema(Version(2, 0), {
        'release': SimpleItem(defvalue='', doc='Software Release'),
        'user_area': FileItem(doc='A tar file of the user area'),
        'testAttribute': ComponentItem('testAttributes')
    })

    _category = 'applications'
    _name = 'TestApplicationMigration'
    _exportmethods = ['prepare', 'setup', 'postprocess']


######### migration ######################################################
    def getMigrationClass(cls, version):
        """This class method returns a (stub) class compatible with the schema <version>.
        Alternatively, it may return a (stub) class with a schema more recent than schema <version>,
        but in this case the returned class must have "getMigrationClass" and "getMigrationObject"
        methods implemented, so that a chain of convertions can be applied."""
        return TestApplication12
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
                if attr == 'user_area':
                    converted_obj.user_area.name = obj.user_area
                else:
                    setattr(converted_obj, attr, getattr(obj, attr))
            return converted_obj
    getMigrationObject = classmethod(getMigrationObject)
######## migration #######################################################

    def setup(self):
        pass

    def postprocess(self):
        pass

    def prepare(self):
        pass

    def configure(self, masterappconfig):
        return (None, None)

    def master_configure(self):
        return (0, None)


###### migration ##############################################################

class TestApplication12(TestApplicationMigration):

    """The test application for the migration framework"""

    _schema = Schema(Version(1, 0), {
        'release': SimpleItem(defvalue='', doc='Software Release'),
        'user_area': SimpleItem(defvalue='', doc='A tar file of the user area'),
        'testAttribute': ComponentItem('testAttributes')
    })

    _category = 'application_converters'
    _name = 'TestApplication12'
    _exportmethods = ['prepare', 'setup', 'postprocess']

    def getMigrationClass(cls, version):
        """This class method returns a (stub) class compatible with the schema <version>.
        Alternatively, it may return a (stub) class with a schema more recent than schema <version>,
        but in this case the returned class must have "getMigrationClass" and "getMigrationObject"
        methods implemented, so that a chain of convertions can be applied."""
        # Your code here
        return
    getMigrationClass = classmethod(getMigrationClass)

    def getMigrationObject(cls, obj):
        """This method takes as input an object of the class returned by the "getMigrationClass" method,
        performs object transformation and returns migrated object of this class (cls)."""
        # Your code here
        return
    getMigrationObject = classmethod(getMigrationObject)
