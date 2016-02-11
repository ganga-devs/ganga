from __future__ import absolute_import

import random
import threading

import Ganga.Utility.Config
from Ganga.Core.GangaRepository.Registry import makeRepository
from Ganga.Runtime.Repository_runtime import getLocalRoot
from Ganga.Utility.logging import getLogger
from Ganga.Core.InternalServices.Coordinator import enableInternalServices, disableInternalServices
from ..GangaUnitTest import GangaUnitTest

config = Ganga.Utility.Config.getConfig('Configuration')

logger = getLogger(modulename=True)


class FakeRegistry(object):
    def __init__(self, name):
        self.name = name
        self.repo = None

    def _read_access(self, root, obj):
        if not obj._data:
            self.repo.load([root._registry_id])

    def _write_access(self, root):
        self.repo.load([root._registry_id])

    def getIndexCache(self, obj):
        return {}

    def _dirty(self, obj):
        self.repo.flush([obj._registry_id])


# def _dirty(self,obj):
#        pass

# Random actions (synchronized by registry per instance!):
# * shutdown + startup
# * update_index (random ID or None, currently only None)
# * ids = add(objs) (check len(ids) == len(objs), add ids to owned_ids)
# * delete(ids) (random list from owned_ids, check if they are gone!)
# * load(ids) (random ids from self.objects.keys())
# * flush(ids) (random owned ids)
# * lock(ids) (random ids, if return True put into owned_ids)
# * unlock(ids) (random owned ids, remove from owned_ids)

class HammerThread(threading.Thread):
    def __init__(self, _id, repo):
        self.id = _id
        self.repo = repo
        self.rng = random.Random()
        self.owned_ids = []
        self.done = False
        super(HammerThread, self).__init__()

    def updown(self):
        logger.info(str(self.id) + ' shutdown()')
        self.repo.shutdown()
        logger.info(str(self.id) + ' shutdown() done!')
        self.owned_ids = []  # locks lapse on shutdown!!!
        logger.info(str(self.id) + ' startup()')
        self.repo.startup()
        logger.info(str(self.id) + ' startup() done!')

    def uindex(self):
        logger.info(str(self.id) + ' update_index(None)')
        self.repo.update_index(None)
        logger.info(str(self.id) + ' update_index(None) done!')

    def load(self):
        n = min(len(self.repo.objects), self.rng.randint(1, 2))
        ids = self.rng.sample(self.repo.objects.keys(), n)
        logger.info(str(self.id) + ' load(%s)' % ids)
        try:
            self.repo.load(ids)
        except KeyError:
            logger.info(str(
                self.id) + ' load(%s) failed - one or more ids were deleted by another thread (if no other thread is running, this is an ERROR!)' % ids)
            return
        for id in ids:
            assert self.repo.objects[id].name
        logger.info(str(self.id) + ' load(%s) done' % ids)

    def lock(self):
        n = min(len(self.repo.objects), self.rng.randint(1, 2))
        ids = self.rng.sample(self.repo.objects.keys(), n)
        logger.info(str(self.id) + ' lock(%s)' % ids)
        lids = self.repo.lock(ids)
        for id in ids:
            if id in self.owned_ids:
                assert id in lids
            else:
                if id in lids:
                    try:
                        self.repo.load([id])
                    except KeyError:  # object is deleted
                        logger.info(str(self.id) + ' locked deleted ID (%s)' % id)
                        continue
                    self.repo.objects[id].name = 'HT%i' % (self.id)
                    self.owned_ids.append(id)
        logger.info(str(self.id) + ' lock(%s) done' % ids)

    def check(self):
        logger.info(str(self.id) + ' check()')
        self.repo.sessionlock.check()
        for id in self.owned_ids:
            assert self.repo.objects[id].name == 'HT%i' % (self.id)
        logger.info(str(self.id) + ' check() done')

    def flush(self):
        n = min(len(self.owned_ids), self.rng.randint(1, 2))
        ids = self.rng.sample(self.owned_ids, n)
        logger.info(str(self.id) + ' flush(%s)' % ids)
        self.repo.flush(ids)
        logger.info(str(self.id) + ' flush() done')

    def add(self):
        from GangaTest.Lib.TestObjects import TestGangaObject  # This import is in here to avoid confusing nosetests
        objs = [TestGangaObject('HT%i' % (self.id)) for i in range(self.rng.randint(1, 2))]
        logger.info(str(self.id) + ' add(%s)' % objs)
        ids = self.repo.add(objs)
        logger.info(str(self.id) + ' add(%s) done, ids = %s!' % (objs, ids))
        assert len(ids) == len(objs)
        # TODO: Check if objects stay the same
        self.repo.flush(ids)
        self.owned_ids.extend(ids)

    def delete(self):
        n = min(len(self.owned_ids), self.rng.randint(1, 2))
        ids = self.rng.sample(self.owned_ids, n)
        logger.info(str(self.id) + ' delete(%s)' % ids)
        self.repo.delete(ids)
        for id in ids:
            assert not id in self.repo.objects
            self.owned_ids.remove(id)
        logger.info(str(self.id) + ' delete(%s) done!' % ids)

    def run(self):
        for i in range(100):
            choices = []
            choices.extend([self.updown] * 1)
            choices.extend([self.uindex] * 2)
            choices.extend([self.add] * 20)
            choices.extend([self.delete] * 15)
            choices.extend([self.load] * 5)
            choices.extend([self.lock] * 5)
            choices.extend([self.check] * 5)
            choices.extend([self.flush] * 2)
            self.rng.choice(choices)()
            assert len(self.owned_ids) == len(dict(zip(self.owned_ids, range(len(self.owned_ids)))).keys())
            for id in self.owned_ids:
                assert id in self.repo.objects
        self.done = True


class TestRepo(GangaUnitTest):
    def setUp(self):
        super(TestRepo, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_pass1(self):
        t = TestRepository(1)
        while not t.isReadyForCheck():
            pass
        returnable = t.checkTest()
        disableInternalServices()
        enableInternalServices()
        return returnable

    def test_pass2(self):
        t = TestRepository(1)
        while not t.isReadyForCheck():
            pass
        returnable = t.checkTest()
        disableInternalServices()
        enableInternalServices()
        return returnable


class TestRepository(object):
    def __init__(self, id):
        self.id = id
        fr = FakeRegistry('TestRegistry')
        fr.type = config['repositorytype']
        fr.location = getLocalRoot()
        self.repo = makeRepository(fr)
        fr.repo = self.repo
        logger.info(str(id) + ' startup()')
        self.repo.startup()
        logger.info(str(id) + ' startup() done!')
        logger.info('RUNNING HAMMERTHREAD ' + str(id))
        self.thread = HammerThread(id, self.repo)
        self.thread.start()

    def isReadyForCheck(self):
        return self.thread.done or not self.thread.isAlive()

    def checkTest(self):
        self.thread.join()
        assert self.thread.done
        logger.info(str(self.id) + ' shutdown()')
        self.repo.shutdown()
        logger.info(str(self.id) + ' shutdown() done!')
