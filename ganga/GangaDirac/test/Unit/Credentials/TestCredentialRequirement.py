
from GangaCore.Utility.Config import getConfig
getConfig('defaults_DiracProxy').addOption('group', 'gridpp_user', '')
getConfig('defaults_DiracProxy').setSessionValue('group', 'gridpp_user')
from GangaDirac.Lib.Credentials.DiracProxy import DiracProxy
from GangaCore.GPIDev.Credentials.AfsToken import AfsToken


def test_encoded():
    v = DiracProxy()
    assert v.encoded() == 'gridpp_user'

    v = DiracProxy(group='foo')
    assert v.encoded() == 'foo'

    a = AfsToken()
    assert a.encoded() == ''


def test_str():
    v = DiracProxy()
    assert str(v).startswith('DiracProxy(')

    a = AfsToken()
    assert str(a) == 'AfsToken()'

    v = DiracProxy(group='foo')
    assert str(v).startswith('DiracProxy(')
    assert "group='foo'" in str(v)


def test_repr():
    v = DiracProxy()
    assert repr(v).startswith('DiracProxy(')
    assert eval(repr(v)) == v

    a = AfsToken()
    assert repr(a) == 'AfsToken()'
    assert eval(repr(a)) == a

    v = DiracProxy(group='foo')
    assert eval(repr(v)) == v


def test_hash():
    v1 = DiracProxy()
    v2 = DiracProxy()
    assert hash(v1) == hash(v2)

    v2 = DiracProxy(group='bar')
    assert hash(v1) != hash(v2)

    a1 = AfsToken()
    a2 = AfsToken()
    assert hash(a1) == hash(a2)

    assert hash(v1) != hash(a1)


def test_eq():
    v1 = DiracProxy()
    v2 = DiracProxy()
    assert v1 == v2

    v2 = DiracProxy(group='bar')
    assert v1 != v2

    a1 = AfsToken()
    a2 = AfsToken()
    assert a1 == a2

    assert v1 != a1

