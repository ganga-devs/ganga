################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: File.py,v 1.2 2008-09-09 14:37:16 moscicki Exp $
################################################################################

import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('Configuration')
gangadir = Ganga.Utility.Config.getConfig('Configuration')['gangadir']
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
import os


from Ganga.Utility.files import expandfilename, chmod_executable, is_executable

class File(GangaObject):
    """Represent the files, both local and remote and provide an interface to transparently get access to them.

    Typically in the context of job submission, the files are copied to the directory where the application
    runs on the worker node. The 'subdir' attribute influances the destination directory. The 'subdir' feature
    is not universally supported however and needs a review.

    """
    _schema = Schema(Version(1,1), {'name': SimpleItem(defvalue="",doc='path to the file source'),
                                    'subdir': SimpleItem(defvalue=os.curdir,doc='destination subdirectory (a relative path)'),
                                    'executable': SimpleItem(defvalue=False,hidden=True,transient=True,
                                                             doc='specify if executable bit should be set when the file is created (internal framework use)')})
    _category = 'files'
    _name = "File"
    #added a subdirectory to the File object. The default is os.curdir, that is "." in Unix.
    #The subdir is a relative path and will be appended to the pathname when writing out files.
    # Therefore changing subdir to a anything starting with "/" will still end up relative
    # to the pathname when the file is copied.
    #
    # There is no protection on putting the parent directory. So ".." is legal and will make
    # the file end up in the parent directory. - AM
    def __init__(self,name=None, subdir=os.curdir):
        super(File, self).__init__()

        if not name is None:
            assert(type(name) is type(''))
            self.name = name 

        if not subdir is None:
            self.subdir = subdir


    def __construct__(self,args):
        if len(args) == 1 and type(args[0]) == type(''):
            v = args[0]
            import os.path
            expanded = expandfilename(v)
            if not urlprefix.match(expanded): # if it is not already an absolute filename
                self.name = os.path.abspath(expanded)
            else: #bugfix #20545 
                self.name = expanded
        else:
            super(File,self).__construct__(args)

    def _attribute_filter__set__(self,attribName,attribValue):
        if attribName is 'name':
            return expandfilename(attribValue)
        return attribValue
    
    def getPathInSandbox(self):
        """return a relative location of a file in a sandbox: subdir/name"""
        from Ganga.Utility.files import real_basename       
        return self.subdir+os.sep+real_basename(self.name)

    def exists(self):
        """check if the file exists (as specified by 'name')"""
        import os.path
        return os.path.isfile(expandfilename(self.name))
        
    def create(self,outname):
        """create a file in  a local filesystem as 'outname', maintain
        the original permissions """
        import shutil

        shutil.copy(expandfilename(self.name),outname)
        if self.executable:
            chmod_executable(outname)
            
    def __repr__(self):
        """Get   the  representation   of  the   file.  Since   the  a
        SimpleStreamer uses  __repr__ for persistency  it is important
        to return  a valid python expression  which fully reconstructs
        the object.  """

        return "File(name='%s',subdir='%s')"%(self.name,self.subdir)

    def isExecutable(self):
        """  return true  if  a file  is  create()'ed with  executable
        permissions,  i.e. the  permissions of  the  existing 'source'
        file are checked"""
        return self.executable or is_executable(expandfilename(self.name))

# add File objects to the configuration scope (i.e. it will be possible to write instatiate File() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['File'] = File

from Ganga.GPIDev.Base.Filters import allComponentFilters

import re
#regex [[PROTOCOL:][SETYPE:]..[<alfanumeric>:][/]]/filename
urlprefix=re.compile('^(([a-zA-Z_][\w]*:)+/?)?/')

def string_file_shortcut(v,item):
    if type(v) is type(''):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        return File._proxyClass(v)._impl
    return None 
        
allComponentFilters['files'] = string_file_shortcut



class ShareDir(GangaObject):
    """Represents the directory used to store resources that are shared amongst multiple Ganga objects.

    Currently this is only used in the context of the prepare() method for certain applications, such as
    the Executable() application. A single ("prepared") application can be associated to multiple jobs.

    """
    _schema = Schema(Version(1,0), {'name': SimpleItem(defvalue='',doc='path to the file source'),
                                    'subdir': SimpleItem(defvalue=os.curdir,doc='destination subdirectory (a relative path)')})
    _category = 'shareddirs'
    _name = "ShareDir"
    _root_shared_path = os.path.join(expandfilename(gangadir),'shared',config['user'])
    def _readonly(self):
        return True

    def __init__(self,name=None,subdir=os.curdir):
        super(ShareDir, self).__init__()
        self._setRegistry(None)

        if not name is None:
            self.name = name
        else:
            #continue generating directory names until we create a unique one (which will likely be on the first attempt).
            while True:
                name = 'conf-' + Ganga.Utility.guid.uuid() 
                shared_path = os.path.join(expandfilename(gangadir),'shared',config['user'])
                if not os.path.isdir(os.path.join(shared_path,name)):
                    os.makedirs(os.path.join(shared_path, name))
                    break
            self.name=str(name)

#    def __construct__(self,args):
#        if len(args) == 1 and type(args[0]) == type(''):
#            v = args[0]
#            import os.path
#            expanded = expandfilename(v)
#            if not urlprefix.match(expanded): # if it is not already an absolute filename
#                self.name = os.path.abspath(expanded)
#            else: #bugfix #20545 
#                self.name = expanded
#        else:
#            super(ShareDir,self).__construct__(args)
        
    def exists(self):
        """check if the file exists (as specified by 'name')"""
        import os.path
        return os.path.isdir(expandfilename(self.name))
        
    def create(self,outname):
        """create a file in  a local filesystem as 'outname', maintain
        the original permissions """
        import shutil

        shutil.copy(expandfilename(self.name),outname)
        if self.executable:
            chmod_executable(outname)
            
    def __repr__(self):
        """Get   the  representation   of  the   file.  Since   the  a
        SimpleStreamer uses  __repr__ for persistency  it is important
        to return  a valid python expression  which fully reconstructs
        the object.  """

        return "ShareDir(name='%s',subdir='%s')"%(self.name,self.subdir)

    def isExecutable(self):
        """  return true  if  a file  is  create()'ed with  executable
        permissions,  i.e. the  permissions of  the  existing 'source'
        file are checked"""
        return self.executable or is_executable(expandfilename(self.name))

import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['ShareDir'] = ShareDir

from Ganga.GPIDev.Base.Filters import allComponentFilters

import re
#regex [[PROTOCOL:][SETYPE:]..[<alfanumeric>:][/]]/filename
urlprefix=re.compile('^(([a-zA-Z_][\w]*:)+/?)?/')

def string_sharedfile_shortcut(v,item):
    if type(v) is type(''):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        return ShareDir._proxyClass(v)._impl
    return None 
        
allComponentFilters['shareddirs'] = string_sharedfile_shortcut
#
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2008/07/17 16:40:53  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.15  2007/08/24 15:55:03  moscicki
# added executable flag to the file, ganga will set the executable mode of the app.exe file (in the sandbox only, the original file is not touched), this is to solve feature request #24452
#
# Revision 1.14  2007/01/25 16:18:21  moscicki
# mergefrom_Ganga-4-2-2-bugfix-branch_25Jan07 (GangaBase-4-14)
#
# Revision 1.12.2.4  2006/11/27 09:30:10  amuraru
# Extended the absolute paths detection in File objects
#
# Revision 1.12.2.3  2006/10/27 15:34:59  amuraru
# *** empty log message ***
#
# Revision 1.12.2.2  2006/10/27 15:33:59  amuraru
# bugs #20545
#
# Revision 1.12  2006/08/29 12:45:08  moscicki
# automatic conversion to absolute path
#
# Revision 1.11  2006/07/27 20:10:45  moscicki
# File to the configuartion symbol scope
#
# Revision 1.10  2006/02/17 14:21:32  moscicki
# fixed exists() method: using expanded file name
#
# Revision 1.9  2006/02/10 14:21:56  moscicki
# added exists() method
# docstring cleanup
#
# Revision 1.8  2005/08/23 17:07:23  moscicki
# Added executable flag for FileBuffer.
# Added create method for File and FileBuffer.
# Added getPathInSandbox() method.
#
# Revision 1.7  2005/08/10 09:45:36  andrew
# Added a subdir to File and FileBuffer objects. Changed the writefile method
# in FileWorspace to use the subdirectory
#
#
#
