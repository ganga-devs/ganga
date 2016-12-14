from __future__ import absolute_import

import copy
import pytest

from Ganga.GPIDev.Base.Objects import _getName
from Ganga.GPIDev.Base.Proxy import stripProxy, GPIProxyClassFactory, setProxyInterface

from Ganga.testlib.decorators import add_config

from Ganga.Utility.Config import getConfig, makeConfig
makeConfig('defaults_DiracProxy', '')
getConfig('defaults_DiracProxy').addOption('group', 'some_group', '')
from GangaDirac.Lib.Credentials.DiracProxy import DiracProxy, DiracProxyInfo
getConfig('defaults_DiracProxy').setSessionValue('group', 'some_group')

from GangaDirac.Lib.Backends.DiracBase import DiracBase
from GangaDirac.Lib.Backends.Dirac import Dirac as origDirac

from Ganga.Utility.Plugin import allPlugins

def getDiracSchema():
    new_schema = DiracBase._schema.inherit_copy()
    new_schema.datadict = copy.deepcopy(new_schema.datadict)
    del new_schema.datadict['credential_requirements']
    return new_schema

class FakeDirac(DiracBase):

    _name = 'Dirac'
    _category = 'backends'
    _schema = getDiracSchema()
    _exportmethods = DiracBase._exportmethods[:]
    _packed_input_sandbox = DiracBase._packed_input_sandbox
    __doc__ = DiracBase.__doc__

    def __init__(self):
        super(FakeDirac, self).__init__()


def removeClassFromGPI(class_):
    """ Remove a given class object from the GPI """
    import Ganga.GPI
    del Ganga.GPI.__dict__[_getName(class_)]
    del allPlugins.all_dict[class_._category][_getName(class_)]
    allPlugins._prev_found = {}


def insertClassIntoGPI(class_):
    """ Insert a given class object into the GPI """

    from Ganga.Runtime.GPIexport import exportToGPI

    exportToGPI(_getName(class_), class_, "Objects")

    allPlugins.add(class_, class_._category, _getName(class_))
    allPlugins._prev_found = {}

@add_config([('TestingFramework', 'AutoCleanup', 'False')])
@pytest.mark.usefixtures('gpi')
class TestJobMigration(object):

    def test_a_JobConstruction(self):
        """ First construct the Job object """

        from Ganga.GPI import Job, jobs

        from Ganga.Utility.Config import getConfig
        assert not getConfig('TestingFramework')['AutoCleanup']

        j = Job(backend=FakeDirac())
        assert len(jobs) == 1
        assert 'credential_requirements' not in stripProxy(j.backend)._schema.datadict

        removeClassFromGPI(FakeDirac)        

    def test_b_JobLoads(self):
        """ Test that loading the Job works """

        insertClassIntoGPI(origDirac)

        from Ganga.GPI import jobs, Dirac

        print str(jobs[-1].backend)
        print str(Dirac())

        assert jobs[-1].backend.credential_requirements is not None

    def test_c_JobIsSerializable(self):
        """ Test that we can represent the job as a string """

        from Ganga.GPI import jobs

        this_jobStr = str(jobs[-1])

        print("Job String:\n%s" % this_jobStr)


