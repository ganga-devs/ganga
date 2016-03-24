from ..GangaUnitTest import GangaUnitTest
from Ganga.GPIDev.Base.Proxy import addProxy, TypeMismatchError
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

    def testAdd(self):
        new_list = self.gangalist + self.filelist

    def testSetItem(self):

        for _ in range(10):
            self.gangalist.append(self._makeRandomTFile())

        self.gangalist[0] = self.filelist
        assert self.gangalist[0] == self.filelist

    def testSetSlice(self):

        for _ in range(10):
            self.gangalist.append(self._makeRandomTFile())

        self.gangalist[0:4] = self.filelist[0:4]
        assert self.gangalist[0:4] == self.filelist[0:4]

    def testAppend(self):
        self.gangalist.append(self.filelist)

    def testExtend(self):
        self.gangalist.extend(self.filelist)

    def testInsert(self):

        for _ in range(10):
            self.gangalist.append(self._makeRandomTFile())

        self.gangalist.insert(5, self.filelist)
