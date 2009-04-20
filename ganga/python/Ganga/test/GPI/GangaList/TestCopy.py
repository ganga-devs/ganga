################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestCopy.py,v 1.1 2009-03-18 13:27:48 wreece Exp $
################################################################################

from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPIDev.Base.Proxy import addProxy,getProxyAttr,isProxy,isType

from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
#GangaList = GangaList._proxyClass

import copy
import random
import string

class TestCopy(GangaGPITestCase):
   
    def _makeRandomString(self):
        str_len = random.randint(3,10)
        s = ''
        for _ in range(str_len):
            s += random.choice(string.ascii_letters)
        return s
            
    def _makeRandomTFile(self):
        name = self._makeRandomString()
        subdir = self._makeRandomString()
        return TFile(name = name, subdir = subdir)


    def testCopy(self):
        
        gl = GangaList()
        
        numberOfFiles = 100
        
        for _ in range(numberOfFiles):
            #add something which is generally not allowed by GangaList
            gl.append([self._makeRandomTFile()])
        
        assert len(gl) == numberOfFiles, 'Right number of files must be made'
        
        gl2 = copy.copy(gl)
        assert gl2 == gl, 'lists must be equal'
        assert gl2 is not gl, 'list must be copies'
        assert gl[0] is gl2[0], 'the references must be copied'
        
        
    def testDeepCopy(self):
        
        gl = GangaList()
        
        numberOfFiles = 100
        
        for _ in range(numberOfFiles):
            #add something which is generally not allowed by GangaList
            gl.append([self._makeRandomTFile()])
        
        assert len(gl) == numberOfFiles, 'Right number of files must be made'
        
        gl2 = copy.deepcopy(gl)
        assert gl2 == gl, 'lists must be equal'
        assert gl2 is not gl, 'list must be copies'
        assert gl[0] is not gl2[0], 'the references must be copied'
       
        