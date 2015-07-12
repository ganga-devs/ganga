##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestTransitions.py,v 1.1 2008-07-17 16:41:16 moscicki Exp $
##########################################################################
from Ganga.GPIDev.Schema import *

from Ganga.Lib.Executable import Executable, RTHandler

# a list of states we must see otherwise the test fails
expected_minimal_states = ['completed', 'running', 'submitted']


class MockExeApplication(Executable):

    _category = 'applications'
    _hidden = 0
    _name = 'MockExeApplication'
    _schema = Executable._schema.inherit_copy()
    _schema.datadict['called'] = SimpleItem(defvalue=False)

    def __init__(self):
        super(MockExeApplication, self).__init__()
        self.called = False

    def transition_update(self, new_status):
        if new_status in expected_minimal_states:
            expected_minimal_states.remove(new_status)
        self.called = True


from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('MockExeApplication', 'Local', RTHandler)

from Ganga.Utility.Plugin import allPlugins
allPlugins.add(MockExeApplication, 'applications', 'MockExeApplication')

from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed


class TestTransitions(GangaGPITestCase):

    def testTransitionsCalled(self):

        m = MockExeApplication()

        j = Job(backend=Local())
        j.application = m

        j.submit()

        assert sleep_until_completed(j), 'Job should complete'
        assert j.status == 'completed'
        assert j.application.called, 'The method should have been called'
        assert len(expected_minimal_states) == 0, 'We should have seen all the minimal states'
