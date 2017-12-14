##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestRootMerger.py,v 1.2 2009-03-18 10:46:01 wreece Exp $
##########################################################################
from __future__ import division
from GangaTest.Framework.tests import GangaGPITestCase


class TestPostProcessor_structure(GangaGPITestCase):

    """
    Test the file checker for protection against bad input and do a standard check.
    """

    def __init__(self):
        self.j = None

    def setUp(self):
        self.j = Job()

    def testPostProcessor_structure(self):

        try:
            self.j.postprocessors = SmartMerger()
        except:
            assert False, 'should be able to set one object'

        assert self.j.postprocessors[
            0], 'should be able to subscript a postprocessor'

        self.j.postprocessors = [FileChecker(), SmartMerger()]
        assert isinstance(
            self.j.postprocessors[0], SmartMerger), 'postprocessors should reorder'

        try:
            self.j.postprocessors.append(Notifier())
        except:
            assert False, 'should be able to append postprocessors'

        try:
            self.j.postprocessors.remove(FileChecker())
        except:
            assert False, 'should be able to remove postprocessors'
