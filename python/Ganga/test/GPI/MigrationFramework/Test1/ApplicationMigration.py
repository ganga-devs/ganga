###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ApplicationMigration.py,v 1.1 2008-07-17 16:41:13 moscicki Exp $
###############################################################################

from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.IApplication import IApplication


class TestApplicationMigration(IApplication):

    """The test application for the migration framework"""

    _schema = Schema(Version(1, 0), {
        'release': SimpleItem(defvalue='', doc='Software Release'),
        'user_area': SimpleItem(defvalue='', doc='A tar file of the user area'),
        'testAttribute': ComponentItem('testAttributes')
    })

    _category = 'applications'
    _name = 'TestApplicationMigration'
    _exportmethods = ['prepare', 'setup', 'postprocess']

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
