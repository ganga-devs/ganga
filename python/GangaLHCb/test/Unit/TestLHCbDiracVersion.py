""" Test the ability to change the LHCbDIRAC version from the configuration"""
from __future__ import absolute_import

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import os, os.path

from GangaCore.Utility.logging import getLogger
from GangaCore.testlib.mark import external
from GangaCore.testlib.GangaUnitTest import GangaUnitTest

from GangaCore.Core.exceptions import PluginError

logger = getLogger(modulename=True)


@external
class TestLHCbDiracVersion(GangaUnitTest):
    """Test how the LHCbDIRAC version is set"""

    def test_wildcard(self):
        """See if version can be specified as a wildcard"""
        from GangaLHCb.Utility.LHCbDIRACenv import select_dirac_version
        version = select_dirac_version('v*')
        assert version[0] == 'v'
        assert len(version) > 1

    def test_dereference(self):
        """Test that soft-links are dereferenced"""

        from GangaLHCb.Utility.LHCbDIRACenv import select_dirac_version

        version = select_dirac_version('prod')
        assert version[0] == 'v'
        assert len(version) > 1

    def test_store(self):
        """Make sure that file with environment is stored GANGADIRACENVIRONMENT env variable"""
        from GangaLHCb.Utility.LHCbDIRACenv import store_dirac_environment

        keep = os.environ.pop('GANGADIRACENVIRONMENT')
        store_dirac_environment()
        assert os.environ.has_key('GANGADIRACENVIRONMENT')
        os.environ['GANGADIRACENVIRONMENT'] = keep

    def test_write_cache(self):
        """Test that cache file is written"""

        from GangaLHCb.Utility.LHCbDIRACenv import store_dirac_environment

        fnamekeep = None
        if os.environ.has_key('GANGADIRACENVIRONMENT'):
            fnamekeep = os.environ['GANGADIRACENVIRONMENT']
            os.rename(fnamekeep, fnamekeep+'.keep')
        store_dirac_environment()
        fname = os.environ['GANGADIRACENVIRONMENT']
        assert os.path.exists(fname)
        assert os.path.getsize(fname)
        os.unlink(fname)
        if fnamekeep:
            os.rename(fnamekeep+'.keep', fnamekeep)
