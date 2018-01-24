import os

import pytest

try:
    from unittest.mock import patch, ANY
except ImportError:
    from mock import patch, ANY

from GangaCore.Core.exceptions import GangaFileError
from GangaCore.Utility.logging import getLogger
from GangaDirac.Lib.Utilities.DiracUtilities import GangaDiracError
from GangaCore.testlib.GangaUnitTest import load_config_files, clear_config

logger = getLogger(modulename=True)


@pytest.yield_fixture(scope='function')
def df():
    load_config_files()

    from GangaDirac.Lib.Files.DiracFile import DiracFile
    f = DiracFile('np', 'ld', 'lfn')
    f.locations = ['location']
    f.guid = 'guid'
    yield f
    clear_config()


def test__init__(df):
    from GangaDirac.Lib.Files.DiracFile import DiracFile

    assert df.namePattern == 'np', 'namePattern not initialised as np'
    assert df.lfn == 'lfn', 'lfn not initialised as lfn'
    assert df.localDir == 'ld', 'localDir not initialised as ld'

    d1 = DiracFile()
    assert d1.namePattern == '', 'namePattern not default initialised as empty'
    assert d1.lfn == '', 'lfn not default initialised as empty'
    assert d1.localDir is None, 'localDir not default initialised as None'
    assert d1.locations == [], 'locations not initialised as empty list'

    d2 = DiracFile(namePattern='np', lfn='lfn', localDir='ld')
    assert d2.namePattern == 'np', 'namePattern not keyword initialised as np, initialized as: %s\n%s' % (d2.namePattern, str(d2))
    assert d2.lfn == 'lfn', 'lfn not keyword initialised as lfn, initialized as: %s\n%s' % (d2.lfn, str(d2))
    assert d2.localDir == 'ld', 'localDir not keyword initialised as ld, initializes as %s\n%s' % (d2.localDir, str(d2.localDir))


def test__attribute_filter__set__(df):
    assert df._attribute_filter__set__('dummyAttribute', 12) == 12, 'Pass through of non-specified attribute failed'
    assert df._attribute_filter__set__('lfn', 'a/whole/newlfn') == 'a/whole/newlfn', "setting of lfn didn't return the lfn value"
    assert df.namePattern == 'newlfn', "Setting the lfn didn't change the namePattern accordingly"
    assert df._attribute_filter__set__('localDir', '~') == os.path.expanduser('~'), "Didn't fully expand the path"


def test__repr__(df):
    assert repr(df) == "DiracFile(namePattern='%s', lfn='%s', localDir='%s')" % (df.namePattern, df.lfn, df.localDir)


def test__auto_remove(df):
    with patch('GangaDirac.Lib.Files.DiracFile.execute') as execute:
        assert df._auto_remove() is None
        execute.assert_called_once_with('removeFile("lfn")', cred_req=ANY)

    with patch('GangaDirac.Lib.Files.DiracFile.execute') as execute:
        df.lfn = ''
        assert df._auto_remove() is None
        execute.assert_not_called()


def test_remove(df):
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'Successful': {'lfn': True}}) as execute:
        assert df.remove() is True
        assert df.lfn == ''
        assert df.locations == []
        assert df.guid == ''
        execute.assert_called_once_with('removeFile("lfn")', cred_req=ANY)

    # Now lfn='' exception should be raised
    with pytest.raises(Exception):
        df.remove()

    df.lfn = 'lfn'

    logger.info("Testing failure when exception is raised")
    with patch('GangaDirac.Lib.Files.DiracFile.execute', side_effect=GangaDiracError('test Exception')):
        with pytest.raises(GangaDiracError):
            assert df.remove()
        assert df.lfn == 'lfn'


def test_replicate(df):
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'Successful': {'lfn': {}}}) as execute:
        assert df.replicate('DEST') is None
        execute.assert_called_once_with('replicateFile("lfn", "DEST", "")', cred_req=ANY)
        assert df.locations == ['location', 'DEST']

    df.locations = ['location']
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'Successful': {'lfn': {}}}) as execute:
        assert df.replicate('DEST', 'location') is None
        execute.assert_called_once_with('replicateFile("lfn", "DEST", "location")', cred_req=ANY)
        assert df.locations == ['location', 'DEST']

        logger.info("Testing failure when exception thrown")
        with patch('GangaDirac.Lib.Files.DiracFile.execute', side_effect=GangaDiracError('test Exception')) as execute:
            with pytest.raises(GangaDiracError):
                assert df.replicate('DEST')
            execute.assert_called_once_with('replicateFile("lfn", "DEST", "")', cred_req=ANY)

    df.lfn = ''
    with pytest.raises(GangaFileError):
        df.replicate('DEST')


def test_get(df):
    with pytest.raises(GangaFileError):
        df.get()

    df.localDir = os.getcwd()
    df.lfn = ''
    with pytest.raises(GangaFileError):
        df.get()

    df.lfn = 'lfn'
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'Successful': {'%s' % df.lfn: True}}) as execute:
        assert df.get() is True
        execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir), cred_req=ANY)

    df.lfn = '/the/root/lfn'
    df.namePattern = ''
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'Successful': {'%s' % df.lfn: True}}) as execute:
        assert df.get() is True
        execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir), cred_req=ANY)
        assert df.namePattern == 'lfn'

    df.lfn = '/the/root/lfn.gz'
    df.compressed = True
    df.namePattern = ''
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'Successful': {'%s' % df.lfn: True}}) as execute:
        assert df.get() is True
        execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir), cred_req=ANY)
        assert df.namePattern == 'lfn'

    def getMetadata(this):
        assert this == df
        df.guid = 'guid'
        df.locations = ['location']

    df.guid = ''
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'Successful': {'%s' % df.lfn: True}}) as execute:
        with patch('GangaDirac.Lib.Files.DiracFile.DiracFile.getMetadata', getMetadata):
            assert df.get() is True
            execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir), cred_req=ANY)
            assert df.guid == 'guid'
            assert df.locations == ['location']

    df.locations = []
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'Successful': {'%s' % df.lfn: True}}) as execute:
        with patch('GangaDirac.Lib.Files.DiracFile.DiracFile.getMetadata', getMetadata):
            assert df.get() is True
            execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir), cred_req=ANY)
            assert df.guid == 'guid'
            assert df.locations == ['location']

    df.guid = ''
    df.locations = []
    with patch('GangaDirac.Lib.Files.DiracFile.execute', return_value={'Successful': {'%s' % df.lfn: True}}) as execute:
        with patch('GangaDirac.Lib.Files.DiracFile.DiracFile.getMetadata', getMetadata):
            assert df.get() is True
            execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir), cred_req=ANY)
            assert df.guid == 'guid'
            assert df.locations == ['location']

    logger.info("Testing failure when an exception is raised")
    with patch('GangaDirac.Lib.Files.DiracFile.execute', side_effect=GangaDiracError('test Exception')) as execute:
        with pytest.raises(GangaDiracError):
            assert df.get()
        execute.assert_called_once_with('getFile("%s", destDir="%s")' % (df.lfn, df.localDir), cred_req=ANY)
