import os, shutil
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.GPIDev.Lib.File import File
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename

def sharedir_handler(app, dir_name, output):
    share_dir = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                             'shared',
                             getConfig('Configuration')['user'],
                             app.is_prepared.name,
                             dir_name)
    for root, dirs, files in os.walk(share_dir):
        subdir = root.replace(share_dir,'')[1:] ## [1:] removes the preceeding /
        if ( type(output) is type([]) ) or ( type(output) is type(GangaList()) ):
            output += [File(name=os.path.join(root,f),subdir=subdir) for f in files]
##             for f in files:
##                 output += [File(name=os.path.join(root,f),subdir=subdir)]
        elif type(output) is type(''):
            for d in dirs:
                if not os.path.isdir(d): os.makedirs(d) 
            for f in files:
                shutil.copy(os.path.join(root,f),
                            os.path.join(output,subdir,f))
        else:
            raise GangaException('output must be either a list to append to or a path string to copy to')

