from ..GangaUnitTest import GangaUnitTest
from Ganga.GPIDev.Base.Proxy import addProxy
from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaList
import random
import string


class TestNestedLists(GangaUnitTest):

    def _makeRandomString(self):
        str_len = random.randint(3, 10)
        s = ''
        for _ in range(str_len):
            s += random.choice(string.ascii_letters)
        return s

    def _makeRandomTFile(self):
        from Ganga.GPI import File
        name = self._makeRandomString()
        subdir = self._makeRandomString()
        return File(name=name, subdir=subdir)

    def setUp(self):
        super(TestNestedLists, self).setUp()
        # make a list of lists containing GangaObjects
        self.filelist = []
        self.gangalist = None
        for _ in range(10):
            self.filelist.append([self._makeRandomTFile() for _ in range(3)])

        # make an empty GangaList
        self.gangalist = addProxy(makeGangaList([]))

    def test_a_Add(self):
        new_list = self.gangalist + self.filelist

        assert len(new_list), len(self.gangalist) + len(self.filelist)

    def test_b_SetItem(self):

        for _ in range(10):
            self.gangalist.append(self._makeRandomTFile())

        self.gangalist[0] = self.filelist
        assert self.gangalist[0] == self.filelist

    def test_c_SetSlice(self):

        for _ in range(10):
            self.gangalist.append(self._makeRandomTFile())

        self.gangalist[0:4] = self.filelist[0:4]
        assert self.gangalist[0:4] == self.filelist[0:4]

    def test_d_Append(self):
        gl_len = len(self.gangalist)
        self.gangalist.append(self.filelist)

        assert len(self.gangalist), gl_len + len(self.filelist)

    def test_e_Extend(self):
        gl_len = len(self.gangalist)
        self.gangalist.extend(self.filelist)

        assert len(self.gangalist), gl_len + len(self.filelist)


    def test_f_Insert(self):

        gl_len = len(self.gangalist)
        for _ in range(10):
            self.gangalist.append(self._makeRandomTFile())

        self.gangalist.insert(5, self.filelist)

        assert len(self.gangalist), gl_len + 10 + len(self.filelist)