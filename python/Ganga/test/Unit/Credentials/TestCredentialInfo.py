import pytest
try:
    import unittest.mock as mock
except ImportError:
    import mock

from Ganga.GPIDev.Credentials2.VomsProxy import VomsProxy, VomsProxyInfo
from Ganga.GPIDev.Credentials2.AfsToken import AfsToken, AfsTokenInfo


class FakeShell(object):
    """
    A mock version of Shell which allows to customise the return values

    Examples:
        >>> s = FakeShell()
        >>> assert s.cmd1.call_count == 0
        >>> assert s.cmd1('foo') == (0, '', '')
        >>> assert s.cmd1.call_count == 1
        >>> assert s.cmd1('something -vo') == (0, 'some_vo', '')
        >>> assert s.cmd1.call_count == 2
    """
    vo = 'some_vo'
    timeleft = 100

    def __init__(self):
        self.cmd1 = mock.Mock(wraps=self._cmd1)
        self.system = mock.Mock(wraps=self._system)

    def _cmd1(self, cmd):
        val = ''

        if '-vo' in cmd:
            val = self.vo
        elif '-timeleft' in cmd:
            val = self.timeleft

        return 0, val, ''

    def _system(self, cmd):
        return 0


@pytest.yield_fixture(scope='function')
def fake_shell(mocker):
    s = mocker.patch('Ganga.GPIDev.Credentials2.VomsProxy.Shell', FakeShell)
    mocker.patch('Ganga.GPIDev.Credentials2.ICredentialInfo.os.path.exists', return_value=False)
    yield s


def test_plain_construct():
    VomsProxyInfo(VomsProxy())
    AfsTokenInfo(AfsToken())


def test_construct(fake_shell):
    req = VomsProxy(vo='some_vo')
    VomsProxyInfo(req)


def test_default_location():
    v = VomsProxyInfo(VomsProxy())
    assert v.location == v.default_location()


def test_location(fake_shell):
    req = VomsProxy(vo='some_vo')
    v = VomsProxyInfo(req)
    assert v.location == v.default_location() + ':' + 'some_vo'


def test_create(fake_shell):
    req = VomsProxy(vo='some_vo')
    v = VomsProxyInfo(req)
    v.create()

    assert v.shell.system.call_count == 1
    cmd = v.shell.system.call_args[0][0]
    assert 'voms-proxy-init' in cmd
    assert '-voms some_vo' in cmd
    assert '-out ' + v.location in cmd


def test_is_valid(fake_shell):
    req = VomsProxy(vo='some_vo')
    v = VomsProxyInfo(req)

    assert v.is_valid()

    fake_shell.timeleft = 0

    assert not v.is_valid()
