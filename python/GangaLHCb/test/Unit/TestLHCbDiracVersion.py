""" Test the ability to change the LHCbDIRAC version from the configuration"""
from __future__ import absolute_import

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import os, os.path

import GangaLHCb.Utility.LHCbDiracEnv
from Ganga.Core.exceptions import PluginError

from Ganga.Utility.logging import getLogger
from Ganga.testlib.mark import external

logger = getLogger(modulename=True)


@external
class TestLHCbDiracVersion(unittest.TestCase):
    """Test how the LHCbDIRAC versino is set"""

    def __init__(self, *args, **kwargs):
        super(TestLHCbDiracVersion, self).__init__(*args, **kwargs)

    def setUp(self):
        super(TestLHCbDiracVersion, self).setUp()

    def test_missing_env(self):
        """Check that missing CMTCONFIG is caught correctly"""
        keep = None
        if os.environ.has_key('CMTCONFIG'):
            keep = os.environ.pop('CMTCONFIG')
        self.assertRaises(PluginError,
                          GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment)
        if keep:
            os.environ['CMTCONFIG'] = keep

    def test_wildcard(self):
        """See if version can be specified as a wildcard"""
        version = GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment('v*')
        assert version[0] == 'v'
        assert len(version) > 1

    def test_dereference(self):
        """Test that soft-links are dereferenced"""
        version = GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment('prod')
        assert version[0] == 'v'
        assert len(version) > 1

    def test_store(self):
        """Make sure that file with environment is stored GANGADIRACENVIRONMENT env variable"""
        keep = os.environ.pop('GANGADIRACENVIRONMENT')
        GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment()
        assert os.environ.has_key('GANGADIRACENVIRONMENT')
        os.environ['GANGADIRACENVIRONMENT'] = keep

    def test_write_cache(self):
        """Test that cache file is written"""
        fnamekeep = None
        if os.environ.has_key('GANGADIRACENVIRONMENT'):
            fnamekeep = os.environ['GANGADIRACENVIRONMENT']
            os.rename(fnamekeep, fnamekeep+'.keep')
        GangaLHCb.Utility.LHCbDiracEnv.store_dirac_environment()
        fname = os.environ['GANGADIRACENVIRONMENT']
        assert os.path.exists(fname)
        assert os.path.getsize(fname)
        os.unlink(fname)
        if fnamekeep:
            os.rename(fnamekeep+'.keep', fnamekeep)
