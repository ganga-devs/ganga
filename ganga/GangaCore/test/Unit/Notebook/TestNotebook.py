

try:
    import unittest2 as unittest
except ImportError:
    import unittest
import random

import string, uuid, os.path

from GangaCore.GPIDev.Base.Proxy import addProxy, getProxyClass, getProxyAttr, isProxy, isType, stripProxy

from GangaCore.Lib.Notebook import Notebook

from GangaCore.Utility.logging import getLogger
logger = getLogger(modulename=True)


class TestNotebook(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestNotebook, self).__init__(*args, **kwargs)

        self.notebook = Notebook()


    def setUp(self):
        super(TestNotebook, self).setUp()

    def testFindTemplate(self):
        name = self.notebook.templatelocation()
        assert os.path.exists(name) 
        
    def testWrapper(self):
        unique = str(uuid.uuid4())
        fb = self.notebook.wrapper([],unique,None,None)

        assert fb.getContents().find(unique) > -1
        assert fb.getContents().find('\#\#\#') == -1 
