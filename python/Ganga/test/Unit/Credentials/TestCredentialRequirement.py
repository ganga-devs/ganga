from Ganga.GPIDev.Credentials2.VomsProxy import VomsProxy
from Ganga.GPIDev.Credentials2.AfsToken import AfsToken


def test_encoded():
    v = VomsProxy()
    assert v.encoded() == ''

    v = VomsProxy(vo='foo', group='bar')
    assert v.encoded() == 'foo:bar'

    a = AfsToken()
    assert a.encoded() == ''


def test_str():
    v = VomsProxy()
    assert str(v) == 'VomsProxy()'

    a = AfsToken()
    assert str(a) == 'AfsToken()'

    v = VomsProxy(vo='foo', group='bar')
    assert str(v).startswith('VomsProxy(')
    assert "vo='foo'" in str(v)
    assert "group='bar'" in str(v)


def test_repr():
    v = VomsProxy()
    assert repr(v) == 'VomsProxy()'
    assert eval(repr(v)) == v

    a = AfsToken()
    assert repr(a) == 'AfsToken()'
    assert eval(repr(a)) == a

    v = VomsProxy(vo='foo', group='bar')
    assert eval(repr(v)) == v


def test_hash():
    v1 = VomsProxy()
    v2 = VomsProxy()
    assert hash(v1) == hash(v2)

    v2 = VomsProxy(vo='foo', group='bar')
    assert hash(v1) != hash(v2)

    a1 = AfsToken()
    a2 = AfsToken()
    assert hash(a1) == hash(a2)

    assert hash(v1) != hash(a1)


def test_eq():
    v1 = VomsProxy()
    v2 = VomsProxy()
    assert v1 == v2

    v2 = VomsProxy(vo='foo', group='bar')
    assert v1 != v2

    a1 = AfsToken()
    a2 = AfsToken()
    assert a1 == a2

    assert v1 != a1
