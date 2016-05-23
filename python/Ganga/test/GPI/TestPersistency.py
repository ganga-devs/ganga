from Ganga.GPIDev.Base.Proxy import stripProxy


def test_export(gpi, tmpdir):
    files = [gpi.LocalFile() for _ in range(100)]
    d = gpi.GangaDataset(files=files)
    fn = str(tmpdir.join('ganga-export'))
    gpi.export(d, fn)


def test_roundtrip(gpi, tmpdir):
    files = [gpi.LocalFile() for _ in range(100)]
    d = gpi.GangaDataset(files=files)
    fn = str(tmpdir.join('ganga-export'))
    gpi.export(d, fn)
    d2 = gpi.load(fn)[0]

    d = stripProxy(d)
    d2 = stripProxy(d2)

    assert type(d) == type(d2)
    assert len(d) == len(d2)
    assert d == d2
