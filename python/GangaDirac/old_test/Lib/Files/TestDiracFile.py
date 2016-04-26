from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPI import DiracFile
import unittest
import tempfile
import os

from Ganga.GPIDev.Base.Proxy import stripProxy

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)


class TestDiracFile(GangaGPITestCase):

    def setUp(self):
        self.returnObject = None
        self.toCheck = {}

        def execute(command, timeout=60, env=None, cwd=None, shell=False):
            import inspect
            frame = inspect.currentframe()
            fedInVars = inspect.getargvalues(frame).locals
            del frame

            for key, value in self.toCheck.iteritems():
                if key in fedInVars:
                    self.assertEqual(fedInVars[key], value)

            return self.returnObject

        def add_process(this, command, command_args=(), command_kwargs={}, timeout=60, env=None, cwd=None, shell=False,
                        priority=5, callback_func=None, callback_args=(), callback_kwargs={}):
            import inspect
            frame = inspect.currentframe()
            fedInVars = inspect.getargvalues(frame).locals
            del frame

            for key, value in self.toCheck.iteritems():
                if key in fedInVars:
                    self.assertEqual(fedInVars[key], value)

            return self.returnObject

        self.df = stripProxy(DiracFile('np', 'ld', 'lfn'))
        self.df.locations = ['location']
        self.df.guid = 'guid'
        from Ganga.Core.GangaThread.WorkerThreads.WorkerThreadPool import WorkerThreadPool
        setattr(sys.modules[self.df.__module__], 'execute', execute)
        setattr(WorkerThreadPool, 'add_process', add_process)

    def test__init__(self):
        self.assertEqual(self.df.namePattern, 'np',  'namePattern not initialised as np')
        self.assertEqual(self.df.lfn,         'lfn', 'lfn not initialised as lfn')
        self.assertEqual(self.df.localDir,    'ld',  'localDir not initialised as ld')

        d1 = stripProxy(DiracFile())
        self.assertEqual(d1.namePattern, '', 'namePattern not default initialised as empty')
        self.assertEqual(d1.lfn,         '', 'lfn not default initialised as empty')
        self.assertEqual(d1.localDir,    None, 'localDir not default initialised as None')
        self.assertEqual(d1.locations,   [], 'locations not initialised as empty list')

        d2 = stripProxy(DiracFile(namePattern='np', lfn='lfn', localDir='ld'))
        self.assertEqual(d2.namePattern, 'np',  'namePattern not keyword initialised as np, initialized as: %s\n%s' % (d2.namePattern, str(d2)) )
        self.assertEqual(d2.lfn, 'lfn', 'lfn not keyword initialised as lfn, initialized as: %s\n%s' % (d2.lfn, str(d2)))
        self.assertEqual(d2.localDir, 'ld',  'localDir not keyword initialised as ld, initializes as %s\n%s' % (d2.localDir, str(d2.localDir)))

    def test__attribute_filter__set__(self):
        self.assertEqual(self.df._attribute_filter__set__('dummyAttribute', 12), 12, 'Pass throught of non-specified attribute failed')
        self.assertEqual(self.df._attribute_filter__set__('lfn', 'a/whole/newlfn'), 'a/whole/newlfn', "setting of lfn didn't return the lfn value")
        self.assertEqual(self.df.namePattern, 'newlfn',"Setting the lfn didn't change the namePattern accordingly")
        self.assertEqual(self.df._attribute_filter__set__('localDir', '~'), os.path.expanduser('~'), "Didn't fully expand the path")

##   enable this if/when DiracFile has implemented this method - rcurrie
#    def test__on_attribute__set__(self):
#        d1 = self.df._on_attribute_filter__set__('', 'dummyAttrib')
#        d2 = self.df._on_attribute__set__(Job()._impl, 'outputfiles')
#        self.assertEqual(d1, self.df, "didn't create a copy as default action")
#        self.assertNotEqual(d2, self.df, "didn't modify properly when called with Job and outputfiles")
#        self.assertEqual(d2.namePattern, self.df.namePattern, 'namePattern should be unchanged')
#        self.assertEqual(d2.localDir, None, "localDir should be blanked")
#        self.assertEqual(d2.lfn, '', "lfn should be blanked")

    def test__repr__(self):
        self.assertEqual(repr(self.df), "DiracFile(namePattern='%s', lfn='%s')" % (self.df.namePattern, self.df.lfn))

    def test__auto_remove(self):
        self.toCheck = {'command': 'removeFile("lfn")',
                        'shell': False,
                        'priority': 7}
        self.assertEqual(self.df._auto_remove(), None)
        self.df.lfn = ''
        self.assertEqual(self.df._auto_remove(), None)

    def test_remove(self):
        self.toCheck = {'command': 'removeFile("lfn")'}
        self.returnObject = {'OK': True, 'Value': {'Successful': {'lfn': True}}}

        self.assertEqual(self.df.remove(),  None)
        self.assertEqual(self.df.lfn,       '')
        self.assertEqual(self.df.locations, [])
        self.assertEqual(self.df.guid,      '')

        # Now lfn='' exception should be raised
        self.assertRaises(Exception, self.df.remove)

        self.df.lfn = 'lfn'

        fail_returns = [('Not Dict',                      'STRING!'),
                        ("No 'OK' present",
                         {'Value': {'Successful': {'lfn': True}}}),
                        ('OK is False',                   {
                         'OK': False, 'Value': {'Successful': {'lfn': True}}}),
                        ("No 'Value' present",            {'OK': True}),
                        ("LFN not in Value['Successful']", {
                         'OK': True, 'Value': {'Successful': {}}})
                        ]

        for label, fr in fail_returns:
            logger.info("Testing failure when return is {0} ...".format(label))
            self.returnObject = fr
            self.assertEqual(self.df.remove(), self.returnObject)
            self.assertEqual(self.df.lfn, 'lfn')
            logger.info("Pass")

    def test_replicate(self):

        self.toCheck = {'command': 'replicateFile("lfn", "DEST", "location")'}
        self.returnObject = {
            'OK': True, 'Value': {'Successful': {'lfn': True}}}
        self.assertEqual(self.df.replicate('DEST'), None)
        self.assertEqual(self.df.locations, ['location', 'DEST'])

        fail_returns = [('Not Dict',                      'STRING!'),
                        ("No 'OK' present",
                         {'Value': {'Successful': {'lfn': True}}}),
                        ('OK is False',                   {
                         'OK': False, 'Value': {'Successful': {'lfn': True}}}),
                        ("No 'Value' present",            {'OK': True}),
                        ("LFN not in Value['Successful']", {
                         'OK': True, 'Value': {'Successful': {}}})
                        ]
        for label, fr in fail_returns:
            logger.info("Testing failure when return is {0} ...".format(label))
            self.returnObject = fr
            self.assertEqual(self.df.replicate('DEST'), self.returnObject)
            logger.info("Pass")

        self.df.lfn = ''
        self.assertRaises(Exception, self.df.replicate, 'DEST')
        self.df.lfn = 'lfn'
        self.df.locations = []
        self.assertRaises(Exception, self.df.replicate, 'DEST')

    def test_get(self):
        import os
        from Ganga.Core.GangaThread.WorkerThreads.WorkerThreadPool import WorkerThreadPool

        self.assertRaises(Exception, self.df.get)
        self.df.localDir = os.getcwd()
        self.df.lfn = ''
        self.assertRaises(Exception, self.df.get)
        self.df.lfn = 'lfn'

        self.toCheck = {
            'command': 'getFile("%s", destDir="%s")' % (self.df.lfn, self.df.localDir)}
        self.returnObject = {
            'OK': True, 'Value': {'Successful': {'%s' % self.df.lfn: True}}}
        self.assertEqual(self.df.get(), None)

        self.df.lfn = '/the/root/lfn'
        self.toCheck = {
            'command': 'getFile("%s", destDir="%s")' % (self.df.lfn, self.df.localDir)}
        self.returnObject = {
            'OK': True, 'Value': {'Successful': {'%s' % self.df.lfn: True}}}
        self.df.namePattern = ''
        self.assertEqual(self.df.get(), None)
        self.assertEqual(self.df.namePattern, 'lfn')

        self.df.lfn = '/the/root/lfn.gz'
        self.toCheck = {
            'command': 'getFile("%s", destDir="%s")' % (self.df.lfn, self.df.localDir)}
        self.returnObject = {
            'OK': True, 'Value': {'Successful': {'%s' % self.df.lfn: True}}}
        self.df.compressed = True
        self.df.namePattern = ''
        self.assertEqual(self.df.get(), None)
        self.assertEqual(self.df.namePattern, 'lfn')

        def getMetadata(this):
            self.assertEqual(this, self.df)
            self.df.guid = 'guid'
            self.df.locations = ['location']
        setattr(DiracFile, "getMetadata", getMetadata)
        self.df.guid = ''
        self.assertEqual(self.df.get(), None)
        self.assertEqual(self.df.guid, 'guid')
        self.assertEqual(self.df.locations, ['location'])

        self.df.locations = []
        self.assertEqual(self.df.get(), None)
        self.assertEqual(self.df.guid, 'guid')
        self.assertEqual(self.df.locations, ['location'])

        self.df.guid = ''
        self.df.locations = []
        self.assertEqual(self.df.get(), None)
        self.assertEqual(self.df.guid, 'guid')
        self.assertEqual(self.df.locations, ['location'])

        fail_returns = [('Not Dict',                      'STRING!'),
                        ("No 'OK' present",
                         {'Value': {'Successful': {'lfn': True}}}),
                        ('OK is False',                   {
                         'OK': False, 'Value': {'Successful': {'lfn': True}}}),
                        ("No 'Value' present",            {'OK': True}),
                        ("LFN not in Value['Successful']", {
                         'OK': True, 'Value': {'Successful': {}}})
                        ]
        for label, fr in fail_returns:
            logger.info("Testing failure when return is {0} ...".format(label))
            self.returnObject = fr
            self.assertEqual(self.df.get(), self.returnObject)
            logger.info("Pass")
