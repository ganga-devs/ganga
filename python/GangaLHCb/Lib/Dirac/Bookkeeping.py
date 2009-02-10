"""
Bookkkeeping.py: The LHCb Bookkeeping interface to Ganga
"""
from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
import DiracShared
import DiracUtils
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
import Ganga.Utility.logging
import os,sys

logger = Ganga.Utility.logging.getLogger()

class Bookkeeping(GangaObject):
#class Bookkeeping:
    _schema=Schema(Version(1,0),{
            'status':SimpleItem(
                defvalue=None,
                protected=1,
                copyable=0,
                typelist=['str','type(None)'],
                doc='''Status of the bookkeeping system'''),
            })
    _exportmethods=['browse']
    _category ='datasets'
    _name = 'Bookkeeping'

    def __init__(self):
        super(Bookkeeping,self).__init__()
        pass

    def _createTmpFile(self):
        import tempfile
        temp_fd,temp_filename=tempfile.mkstemp(text=True,suffix='.txt')
        os.write(temp_fd,'')
        os.close(temp_fd)
        return temp_filename

    def browse(self,gui=True):
        from DiracScript import DiracScript
        from GangaLHCb.Lib.LHCbDataset.LHCbDataset import string_dataset_shortcut
        f=self._createTmpFile()
        if gui:
            dw=diracwrapper(DiracUtils.bookkeeping_browse_command(f))
            rc= dw.getOutput()
            l=self.fileToList(f)
            ds=string_dataset_shortcut(l,None)
            return ds
    
    def fileToList(self,file):
        f=open(file)
        l=f.read().splitlines()
        f.close()
        return l
