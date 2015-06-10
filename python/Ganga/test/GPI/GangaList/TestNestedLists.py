from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPIDev.Base.Proxy import addProxy,getProxyAttr,isProxy,isType, TypeMismatchError
from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaList
import os
import pickle
import random
import string
import tempfile


class TestNestedLists(GangaGPITestCase):
    
    def _makeRandomString(self):
        str_len = random.randint(3,10)
        s = ''
        for _ in range(str_len):
            s += random.choice(string.ascii_letters)
        return s
            
    def _makeRandomTFile(self):
        name = self._makeRandomString()
        subdir = self._makeRandomString()
        return File(name = name, subdir = subdir)
    
    
    def __init__(self):
        
        self.filelist = []
        self.gangalist = None
    
    def setUp(self):
        
        #make a list of lists containing GangaObjects
        self.filelist = []
        for _ in range(10): self.filelist.append([self._makeRandomTFile() for _ in range(3)])
        
        #make an empty GangaList
        self.gangalist = addProxy(makeGangaList([]))
    
    def testAdd(self):
        
        try:
            self.gangalist + self.filelist
            assert False, 'Exception should be thrown'
        except TypeMismatchError as e:
            pass
        
    def testSetItem(self):
        
        for _ in range(10): self.gangalist.append(self._makeRandomTFile())
        
        try:
            self.gangalist[0] = self.filelist
            assert False, 'Exception should be thrown'
        except TypeMismatchError as e:
            pass
    
    def testSetSlice(self):
        
        for _ in range(10): self.gangalist.append(self._makeRandomTFile())
        
        try:
            self.gangalist[0:4] = self.filelist[0:4]
            assert False, 'Exception should be thrown'
        except TypeMismatchError as e:
            pass
            
    def testAppend(self):
        
        try:
            self.gangalist.append(self.filelist)
            assert False, 'Exception should be thrown'
        except TypeMismatchError as e:
            pass
            
    def testExtend(self):
        
        try:
            self.gangalist.extend(self.filelist)
            assert False, 'Exception should be thrown'
        except TypeMismatchError as e:
            pass
            
    def testInsert(self):
        
        for _ in range(10): self.gangalist.append(self._makeRandomTFile())
        
        try:
            self.gangalist.insert(5,self.filelist)
            assert False, 'Exception should be thrown'
        except TypeMismatchError as e:
            pass
            
    
        