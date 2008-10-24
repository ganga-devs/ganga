"""
Bookkkeeping.py: The LHCb Bookkeeping interface to Ganga
"""
import GangaLHCb.Lib.Dirac.DiracWrapper as DiracWrapper
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
import Ganga.Utility.logging
import os,sys

logger = Ganga.Utility.logging.getLogger()

#class Bookkeeping(GangaObject):
class Bookkeeping:
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
#        super(Bookkeeping,self).__init__()
        pass

    def browse(self,gui=True):
        s=DiracWrapper.s
        if gui:
            rc=s.cmd1('python $DIRACROOT/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-gui.py')
            return self.extractOptsfile('''myopts.optsption file (*.opts);;*.py;;*.txt''')    
        else:
            tf=self.bkkscript()
            rc=s.system('python -i ' + tf)
            os.unlink(tf)
            return self.extractOptsfile('''myopts.opts''')
            
            
    
    def bkkscript(self):
        import tempfile
        script='''
        
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient  import LHCB_BKKDBClient        
print "You are now leaving the Ganga prompt and are entering the command line interface"
print "of the Dirac bookkeeping system. Once you have finished and save your selections,"
print "quit the bookeeping system using Ctrl-D"

print ''
print'Creating an instance of the bookkeeping object in variable "b"'

b=LHCB_BKKDBClient()
'''
        temp_fd,temp_filename=tempfile.mkstemp(text=True)
        os.write(temp_fd,script)
        os.close(temp_fd)
        return temp_filename
    def extractOptsfile(self,file):
        f=open(file)
        list=f.read().splitlines()
        lfns=[]
        for i in list:
            index=i.find('LFN')
            if index<>-1:
                i=i[index:]
                index=i.find("""'""")
                if index<>-1:
                    i=i[:index]
                    lfns.append(i)
        return lfns
