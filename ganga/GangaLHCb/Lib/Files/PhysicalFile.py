
# PhysicalFile is now a pseudonym for the LocalFile... This is for backwards
# compatability in the simplest of cases, where the user needs to do something more
# complicated they're encouraged to update their code

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


import os
import re
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from GangaCore.Utility.files import expandfilename
from GangaCore.Core.exceptions import GangaException
from GangaCore.GPIDev.Base.Proxy import GPIProxyObjectFactory
from GangaCore.GPIDev.Lib.File.LocalFile import LocalFile
import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def full_expand_filename(name):
    if len(name) >= 4 and name[0:4].upper() == 'LFN:':
        msg = 'Can not create PhysicalFile from string that begins w/ "LFN:".'\
              ' You probably want to create a DiracFile.'
        raise GangaException(msg)
    urlprefix = re.compile(r'^(([a-zA-Z_][\w]*:)+/?)?/')
    if len(name) >= 4 and name[0:4].upper() == 'PFN:':
        name = name[4:]
    expanded_name = expandfilename(name)
    if urlprefix.match(expanded_name):
        return expanded_name
    return os.path.abspath(expanded_name)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class PhysicalFile(LocalFile):

    '''Class for handling physical files (i.e. PFNs)

    Example Usage:
    pfn = PhysicalFile("/some/pfn.file")
    pfn.upload("/some/lfn.file","CERN-USER") # upload the PFN to LFC
    [...etc...]
    '''
    _schema = Schema(Version(1, 0), {'name': SimpleItem(defvalue='', doc='PFN'),
                                     'namePattern': SimpleItem(defvalue="", doc='pattern of the file name', transient=1),
                                     'localDir': SimpleItem(defvalue="", doc='local dir where the file is stored, used from get and put methods', transient=1),
                                     'subfiles': ComponentItem(category='gangafiles', defvalue=[], hidden=1, typelist=['GangaCore.GPIDev.Lib.File.LocalFile'],
                                                               sequence=1, copyable=0, doc="collected files from the wildcard namePattern", transient=1),
                                     'compressed': SimpleItem(defvalue=False, typelist=['bool'], protected=0, doc='wheather the output file should be compressed before sending somewhere', transient=1)})
    _category = 'gangafiles'
    _name = 'PhysicalFile'
    _exportmethods = ['location', 'upload']

    def __init__(self, name=''):
        val = full_expand_filename(name)
        super(PhysicalFile, self).__init__(namePattern=val)
        self.namePattern = os.path.basename(name)
        self.localDir = os.path.dirname(val)
        self.name = val
        logger.warning(
            "!!! PhysicalFile has been deprecated, this is now just a wrapper to the LocalFile object")
        logger.warning(
            "!!! Please update your scripts before PhysicalFile is removed")

    def _attribute_filter__set__(self, n, v):
        if n == 'name':
            import os.path
            val = full_expand_filename(v)
            self.name = val
            self.namePattern = os.path.basename(val)
            self.localDir = os.path.dirname(val)
            return val
        return v

    def upload(self, lfn, diracSE, guid=None):

        from GangaDirac.Lib.Files.DiracFile import DiracFile
        diracFile = DiracFile(namePattern=self.name, lfn=lfn)

        diracFile.put(force=True)

        return diracFile

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
