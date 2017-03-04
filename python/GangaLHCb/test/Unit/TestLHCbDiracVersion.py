from __future__ import absolute_import

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import os, os.path

import GangaLHCb.Utility.LHCbDiracEnv
from Ganga.Core.exceptions import PluginError

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)


@external
class TestLHCbDiracVersion(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestLHCbDiracVersion, self).__init__(*args, **kwargs)

    def setUp(self):
        super(TestLHCbDiracVersion, self).setUp()

    def test_missing_env(self):
        keep = None
        if os.environ.has_key('CMTCONFIG'):
            keep=os.environ.pop('CMTCONFIG')
        self.assertRaises(PluginError,
          GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment)
        if keep:
            os.environ['CMTCONFIG']=keep

    def test_wildcard(self):
        version = GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment('v*')
        assert(version[0]=='v')
        assert(len(version)>1)
        
    def test_dereference(self):
        version = GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment('prod')
        assert(version[0]=='v')
        assert(len(version)>1)

    def test_store(self):
        keep=os.environ.pop('GANGADIRACENVIRONMENT')
        GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment()
        assert(os.environ.has_key('GANGADIRACENVIRONMENT'))
        os.environ['GANGADIRACENVIRONMENT']=keep
        
    def test_write_cache(self):
        fnamekeep = None
        if os.environ.has_key('GANGADIRACENVIRONMENT'):
            fnamekeep = os.environ['GANGADIRACENVIRONMENT']
            os.rename(fnamekeep,fnamekeep+'.keep')
        GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment()
        fname = os.environ['GANGADIRACENVIRONMENT']
        assert(exists(fname))
        assert(getsize(fname))
        os.unlink(fname)
        if fnamekeep:
            os.rename(fnamekeep+'.keep',fnamekeep)
