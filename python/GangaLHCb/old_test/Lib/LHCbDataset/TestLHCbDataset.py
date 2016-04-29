import time
from GangaTest.Framework.tests import GangaGPITestCase

from Ganga.Utility.Config import getConfig

import Ganga.Utility.Config.Config

from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *


def make_dataset(files):
    ds = LHCbDataset(files)
    return ds


def make_new_dataset(files):
    ds = LHCbDataset(files)
    return ds


class TestLHCbDataset(GangaGPITestCase):

    def test_LHCbDataset___len__(self):
        ds = LHCbDataset()
        assert len(ds) == 0
        if not getConfig('Output')['ForbidLegacyInput']:
            ds = make_dataset(['pfn:a'])
        else:
            ds = make_new_dataset([LocalFile('/file/path/someFile')])
        assert len(ds) == 1

    def test_LHCbDataset_isEmpty(self):
        ds = LHCbDataset()
        assert ds.isEmpty(), 'dataset is empty'
        if not getConfig('Output')['ForbidLegacyInput']:
            ds = make_dataset(['pfn:a'])
        else:
            ds = make_new_dataset([LocalFile('/file/path/someFile')])
        assert not ds.isEmpty(), 'dataset is not empty'

    def test_hasLFNs(self):
        if not getConfig('Output')['ForbidLegacyInput']:
            ds = make_dataset(['lfn:a'])
            assert ds.hasLFNs()
            if not getConfig('Output')['ForbidLegacyInput']:
                ds = make_dataset(['pfn:a'])
            else:
                ds = make_dataset(['a'])
            assert not ds.hasLFNs()
        else:
            ds = make_new_dataset([DiracFile(lfn='a')])
            assert ds.hasLFNs()
            ds = make_new_dataset([LocalFile('/some/local/file')])
            assert ds.hasPFNs()

    def test_extend(self):
        if not getConfig('Output')['ForbidLegacyInput']:
            ds = make_dataset(['lfn:a'])
            ds.extend(['lfn:b'])
            assert len(ds) == 2
            ds.extend(['lfn:b'])
            assert len(ds) == 3
            ds.extend(['lfn:b'], True)
            assert len(ds) == 3
        else:
            ds = make_new_dataset([DiracFile(lfn='a')])
            ds.extend([DiracFile(lfn='b')])
            assert len(ds) == 2
            ds.extend([DiracFile(lfn='c')])
            assert len(ds) == 3
            ds.extend([DiracFile(lfn='c')], True)
            print("%s" % ds.files)
            assert len(ds) == 3

    def test_getLFNs(self):
        if not getConfig('Output')['ForbidLegacyInput']:
            ds = make_new_dataset([DiracFile(lfn='a'), DiracFile(lfn='b'), LocalFile('c')])
        else:
            ds = make_dataset(['lfn:a', 'lfn:b', 'pfn:c'])
        assert len(ds.getLFNs()) == 2

    def test_getLFNs(self):
        if not getConfig('Output')['ForbidLegacyInput']:
            ds = make_dataset(['lfn:a', 'lfn:b', 'pfn:c'])
        else:
            ds = make_dataset(['lfn:a', 'lfn:b', 'c'])
        assert len(ds.getPFNs()) == 1

    def test_getFileNames(self):
        if not getConfig('Output')['ForbidLegacyInput']:
            ds = make_dataset(['lfn:a', 'lfn:b', 'pfn:c'])
        else:
            ds = make_dataset(['lfn:a', 'lfn:b', 'c'])
        assert len(ds.getFileNames()) == 3

    def test_optionsString(self):
        if not getConfig('Output')['ForbidLegacyInput']:
            ds = make_dataset(['lfn:a', 'lfn:b', 'pfn:c'])
        else:
            ds = make_dataset(['lfn:a', 'lfn:b', 'c'])
        this_str = ds.optionsString()
        print("---\n%s\n---\n" % this_str)
        assert this_str.find('LFN:a') >= 0
        assert this_str.find('LFN:b') >= 0
        if getConfig('Output')['ForbidLegacyInput']:
            assert this_str.find('PFN:') >= 0
        else:
            assert this_str.find('c') >= 0

    # test rest in GPI

