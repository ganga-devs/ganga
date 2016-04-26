
from GangaTest.Framework.tests import GangaGPITestCase


class TestJobProperties(GangaGPITestCase):

    def test001_JobAttributes(self):
        # CHANGE
        job = Job(name='x')
        assert(job.name == 'x')
        assert(job.application is not None)
        assert(job.backend is not None)

        # CHANGE: cannot delete attributes
        try:
            del job.nonexistantattribute
            assert(not "should not reach here, nonexistant attribute deleted!")
        except AttributeError as x:
            pass

        try:
            job.nonexistantattribute_kvmndaio = 10
            assert(
                not "should not reach here, nonexistant attribute has been set!")
        except AttributeError as x:
            pass

        # CHANGE: AttributeError instead of ProtectedAttributeError
        try:
            del job.id
            assert(not "should not reach here, attribute deleted!")
        except AttributeError as x:
            pass

        try:
            job.id = 10
            assert(not "should not reach here, attribute overriden!")
        except AttributeError as x:
            pass

        # CHANGE:
        # you cannot set garbage to application
        try:
            job.application = None
            assert(
                not "should not reach here, garbage assigned to the application component")
        except GangaAttributeError as x:
            pass

    def test004_CopyModule(self):
        """ Test interactions with copy module.
        """
        # CHANGE: "myattr" replaced by "name" because it is not possible to
        # assign random attributes anymore
        j = Job(backend="Batch", application='Executable')
        j.name = 'hello'
        j.application.exe = 'my.exe'
        j.application.args = ['opt', File('x')]
        j.backend.queue = 'myqueue'

        assert(len(jobs) == 1)
        import copy

        try:
            copy.copy(j)
            assert('shallow copy of job should be prohibited')
        except copy.Error:
            pass

        def assert_original(job):
            assert(job.name == 'hello')
            assert(job.application.exe == 'my.exe')
            assert(job.application.args == ['opt', File('x')])
            assert(job.backend.queue == 'myqueue')

        # make sure job.copy() duplicates the attributes but not the internal
        # state (id)
        j2 = j.copy()
        assert_original(j2)
        assert(j2 == j)

        # CHANGE: cannot deepcopy jobs either!!

        try:
            copy.deepcopy(j2)
            assert('deep copy of job should be prohibited')
        except copy.Error:
            pass

        # make sure properties are not shared between the copies
        j.name = '1'
        j2.name = '2'

        assert(j.name == '1')
        assert(j2.name == '2')

    def test005_ComponentAssignment(self):
        j = Job()
        j2 = Job(application=Executable())
        j2.application.exe = 'b'

        # do the assignment of application object
        # this operation should copy all properties
        # but should not use references
        j.application = j2.application

        assert(j.application is not j2.application)
        assert(j.application == j2.application)

    def test006_ComponentAssignment2(self):
        # similar test as above but slightly more complex:
        # component objects are of different class

        j = Job(backend="Local")
        j2 = Job(backend="Batch")
        j2.backend.queue = 'b'

        # CHANGE: comparing types is more "pythonic"
        assert(not isinstance(j.backend, type(j2.backend)))

        j.backend = j2.backend

        assert(isinstance(j.backend, type(j2.backend)))

        assert(j.backend.queue == 'b')

    # NEW
    def test007_ComponentAssignment3(self):
        # check the assignment of jobless components

        j = Job(backend="Local")
        assert(isinstance(j.backend, Local))

        b = Batch(queue='b')
        assert(b.queue == 'b')

        j.backend = b

        assert(isinstance(j.backend, Batch))
        assert(j.backend.queue == 'b')

        b.queue = 'c'

        assert(b.queue == 'c')
        assert(j.backend.queue == 'b')

        j.backend.queue = 'd'

        assert(b.queue == 'c')

    def test008_CopyConstructor(self):
        try:
            Job(File())
        except ValueError:
            pass
        else:
            try:
                Job(File())
            except e:
                raise e
            assert 0, 'schema violation in copy constructor not raising exception'
