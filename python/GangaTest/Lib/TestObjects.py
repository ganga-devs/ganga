from GangaCore.GPIDev.Base.Objects import GangaObject
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem


# Test Ganga Object. 
# testing for 'subjobs', strings and other stuff
class TestGangaObject(GangaObject):
    """Test Ganga Object. Is used to construct test jobs"""
    _schema = Schema(Version(1,0), {
        'id': SimpleItem('0', doc='ID Needed for tests'),
        'name':SimpleItem('',doc='optional label which may be any combination of ASCII characters',typelist=['str']),
        'subjobs':ComponentItem('internal',defvalue=[],sequence=1,protected=1,load_default=0,copyable=0,optional=1,doc='test subjobs'),
    })
    _name   = "TestGangaObject"
    _category = "internal"

    def __init__(self, name='TestObjectName', sj=0):
        super(TestGangaObject,self).__init__()
        self.name = name
        for i in range(sj):
            self.subjobs.append(TestGangaObject(name+"."+str(i)))
