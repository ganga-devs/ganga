#!/usr/bin/env python

# $Id: LFCTools.py,v 1.1 2008-10-11 15:41:13 pajchel Exp $
# $Log: not supported by cvs2svn $
# Revision 1.1  2008/05/07 14:25:38  alread
# lfc output registration, new srm-path for output files, ad32 support and registration in DQ2
#

#
# Cut and apsted by David Cameron, May 2008, from DQ2 site services codebase
#

try:
    import lfc
except:
    pass
import errno
import os
import re

# LFC retry settings
os.environ['LFC_CONNTIMEOUT'] = '30'
os.environ['LFC_CONRETRY'] = '1'
os.environ['LFC_CONRETRYINT'] = '10'

LFC_HOME = '/grid/atlas/'

class LFCFileCatalogException:
    """
    Class representing a LFC file catalog exception.
    
    @see: L{dq2.filecatalog.FileCatalogException}
    
    @author: Miguel Branco <miguel.branco@cern.ch>
    """    
    
    def __init__(self, description):
        """
        Constructor for the LFCFileCatalogException object.
        """
        self._description = description
        
    def __str__(self):
        """
        String representation of the LFCFileCatalogException exception.
        """
        return "LFC exception [%s]" % self._description


def to_native_lfn(dsn, lfn, prefix='dq2/'):
    """
    Return LFN with LFC hierarchical namespace.
    
    The convention relies on the dataset name containing 5 or more
    fields separated by dots. For these datasets the directory
    structure is

    /project/datasettype/datasetname/lfn

    where
    project is the first field
    datasettype is the fifth field
    datasetname is the DQ2 dataset name whose subscription caused this
        file to be copied
    lfn is the flat lfn

    Example:
      toNativeLFN('csc11.005009.J0_pythia_jetjet.digit.RDO.v11004203',
                  'csc11.005009.J0_pythia_jetjet.digit.RDO.v11004203._009911.pool.root')

    returns
      /csc11/RDO/csc11.005009.J0_pythia_jetjet.digit.RDO.v11004203/csc11.005009.J0_pythia_jetjet.digit.RDO.v11004203._009911.pool.root

    Dataset names with between 1 and 4 dots inclusive will go to

    /project/datasetname/lfn

    where these have the same meaning as above

    Example:
      toNativeLFN('user.joeuser.datasettest1', 'joetestfile.1')

    returns
      /user/user.joeuser.datasettest1/joetestfile.1

    A dataset name with no dots goes to the other/ directory

    /other/datasetname/lfn

    Example:
      toNativeLFN('mydataset1', 'file1')

    returns
      /other/mydataset1/file1
    
    
    """
    
    bpath = LFC_HOME
    if bpath[-1] != '/': bpath += '/'
    
    # add prefix
    bpath += prefix
    if bpath[-1] == '/': bpath = bpath[:-1]

    # check how many dots in dsn
    dots = dsn.split('.')

    # if 'correct' convention
    if len(dots) > 4:
        project = dots[0]
        type = dots[4]
        return '%s/%s/%s/%s/%s' % (bpath, project, type, dsn, lfn)

    # if no dots
    elif len(dots) == 1:
        return '%s/other/%s/%s' % (bpath, dsn, lfn)

    # some dots eg user.name.something datasets
    else:
        project = dots[0]
        return '%s/%s/%s/%s' % (bpath, project, dsn, lfn)


def connect(host):
    """
    @see L{dq2.filecatalog.FileCatalogInterface.connect()}
        """
    if lfc.lfc_startsess(host, '') != 0:
        raise LFCFileCatalogException('Cannot connect to LFC [%s]' % host)
    
def disconnect():
    """
    @see L{dq2.filecatalog.FileCatalogInterface.disconnect()}
    """
    lfc.lfc_endsess()        


def createPath(dsn, lfn, start=0):
    """
    Create path on LFC.
    
    First goes down from higher path until it creates successfully
    a directory. Then goes back up to create all missing directories
    in a recursive way.
    """    

    flfn = to_native_lfn(dsn, lfn)        
    dirs = flfn[:flfn.rfind('/')][len('/grid/atlas/'):].split('/')
    
    # build up directory starting from upper level
    for i in xrange(start, len(dirs)):
            
        if i == 0:
            path = '/grid/atlas/'+('/'.join(dirs))
        else:
            path = '/grid/atlas/'+('/'.join(dirs[:-i]))

        s = lfc.lfc_mkdir(path, 0775)
        if s == 0:
            if i == 0:
                return True
            else:
                return createPath(dsn, lfn, start=i-1)
            
        errcode = lfc.cvar.serrno
        if errcode == errno.ENOENT:
            # Path does not exist is acceptable
            continue
        else:
            raise LFCFileCatalogException("ERROR Could not create path with error %s [%s]" % \
                                          (lfc.sstrerror(errcode), path))
        
def createFile(dsn, lfn, guid, surl, bit, fsize, checksum, pathCreation=True):
    """
    Creates a file (LFN + GUID + SURL) entry on LFC.

    @param dsn: The dataset name.
    @param lfn: The file's logical file name.
    @param guid: The file's GUID.
    @param surl: The file's SURL.
    @param bit: The archival bit (P or V).
    @param fsize: The file size.
    @param checksum: The file's checksum.
        
    @return: True if file added.
    """

    s = lfc.lfc_creatg(to_native_lfn(dsn, lfn), guid, 0775)
    if s == 0:
        csumvalue = ''
        csumtype = ''
        if checksum not in [None, '', 0]:
            if checksum[:4] == 'md5:':
                csumtype = 'MD'
                csumvalue = checksum[4:]
            elif checksum[:3] == 'ad:':
                csumtype = 'AD'
                csumvalue = checksum[3:]
            
        fsizevalue = None
        if fsize not in [None, '', 0]:
            fsizevalue = long(fsize)
            
        if fsizevalue is not None or csumvalue != '':
            s = lfc.lfc_setfsizeg(guid, fsizevalue, csumtype, csumvalue)
            if s != 0:
                errcode = lfc.cvar.serrno
                raise LFCFileCatalogException("ERROR Could not set file size with error %s [%s]" % \
                                              (lfc.sstrerror(errcode), to_native_lfn(dsn, lfn)))
            
        # now add replica to newly created file
        return addReplica(guid, surl, bit)
        
    # failed to create file
    errcode = lfc.cvar.serrno
        
    if errcode == errno.EEXIST:
        # we have a problem as guid does not exist according
        # to previous call to _addReplica(..) but lfn exists!
        raise LFCFileCatalogException('ERROR Suspect LFN exists with different GUID [%s -> %s]' % \
                                      (to_native_lfn(dsn, lfn), guid))
        
    elif pathCreation and errcode == errno.ENOENT:
        # create path and try again
        createPath(dsn, lfn)
        return createFile(dsn, lfn, guid, surl, bit, fsize, checksum, pathCreation=False)
                
    # unknown error
    raise LFCFileCatalogException('ERROR Unexpected error %s [%s -> %s]' % \
              (lfc.sstrerror(errcode), to_native_lfn(dsn, lfn), guid))


def addReplica(guid, surl, bit):
    """
    Add replica to an existing file.
    
    Addition is done within a single transaction.
    
    @param guid: The file's GUID.
    @param surl: The file's SURL
    @param bit: The archival bit (P or V).
    
    @return: True if replica added.
    """
        
    # try to create replica
    s = lfc.lfc_addreplica( guid, None, get_hostname(surl), surl, '-', bit, '', '')
    if s == 0:
        return True
        
    errcode = lfc.cvar.serrno
    if errcode == errno.EEXIST:
        # replica exists is safe to ignore
        return True
        
    raise LFCFileCatalogException("ERROR Could not add replica with error %s [%s -> %s]" % \
          (lfc.sstrerror(errcode), guid, surl))


def get_hostname (surl):
    """
    Returns string with hostname or empty
    string if hostname could not be derived
    
    @param surl: URL.
    @type surl: str
    
    @return: hostname.
    @rtype: str
    """
    reg = re.search('[^:]+:(/)*([^:/]+)(:[0-9]+)?(/)?.*', surl)
    host = ''
    try:
        host = reg.group(2)
    except:
        pass
    
    return host


def bulkRegisterFiles(host, files):
    """
    Register files on local replica catalog.
        
        'files' is given by a dictionary mapping guid to a 
        dictionary with dsn, lfn, surl, fsize, checksum and archival bit.

        Example:
            { 'c529b2e3-7427-435e-ba8a-c1eb294ed694':
                {'dsn': 'user.joeuser.1234',
                 'lfn': 'myuserlfn.1',
                 'surl': srm://srm.ndgf.org/pnfs/ndgf.org/data/atlas/disk/test/myuserlfn1',
                 'fsize': 1000
                 'checksum': 'ad:1a967864',
                 'archival': 'P'
                 }
            }
        
        Example response:
            { 'a_guid_1': True,
              'a_guid_2': True,
              'a_missing_guid_1': LFCReplicaCatalogException('some error') }

        Returns dictionary of guid mapping to True if successful
        or exception if error occurred during registration.

        Returns dictionary with all found guids mapping to
        dictionary with lfn, surls, fsize and checksum. If error happened
        for some guid, the mapping is from guid to exception object. If
        guid was not found in catalog, it is not included in the response
        dictionary.

        @param host: LFC host to connect to
        @param files: GUID mapping to dictionary with file attributes such
                      as 'dsn', 'lfn', 'surl', 'fsize', 'checksum' and 'archival'.
        
        @return: Dictionary (see above for details)
    
    """

    connect(host)

    r = {}
    for guid in files:
        dsn = files[guid]['dsn']
        lfn = files[guid]['lfn']
        surl = files[guid]['surl']
        fsize = files[guid]['fsize']
        checksum = files[guid]['checksum']

        # set replica bit to permanent or volatile
        if files[guid]['archival'] == 'P':
            bit = 'P'
        else:
            bit = 'V'
            
        # try to create only the replica first
        try:
            r[guid] = addReplica(guid, surl, bit)
        except LFCFileCatalogException:
            # failed so try to create file
            try:
                r[guid] = createFile(dsn, lfn, guid, surl, bit, fsize, checksum)
            except LFCFileCatalogException, e:
                r[guid] = e
        
    disconnect()
        
    return r

def bulkFindReplicas(host, files):
    """
    Find replicas for files on site. Files is given
    by a dictionary mapping guid to lfn. For missing
    files on catalog no information is returned.

        Example query:
            { 'a_guid_1': 'a_lfn_1',
              'a_missing_guid_1': 'a_lfn_2',
              'a_bad_guid_1': 'a_lfn_3' }              

        Example response:
            { 'a_guid_1': { 'lfn': 'a_lfn_1',
                            'surls': ['srm://a_file'],
                            'fsize': 10000,
                            'checksum': None },
            'a_bad_guid_1': LFCReplicaCatalogException('some error') }

    Note that 'a_missing_guid1' is not returned.

    Returns dictionary with all found guids mapping to
    dictionary with lfn, surls, fsize and checksum. If error happened
    for some guid, the mapping is from guid to exception object. If
    guid was not found in catalog, it is not included in the response
    dictionary.

    @param host: The LFC host to connect to
    @param files: Dictionary mapping each GUID to LFN.
    
    @return: Dictionary (see above for details)
    """
    connect(host)
    nfiles = {}
    
    # do bulk search of GUIDs
    (result, list) = lfc.lfc_getreplicas(files.keys(), '')
    if result != 0:
        errcode = lfc.cvar.serrno
        disconnect()
        raise LFCFileCatalogException('Failed bulk files lookup on LFC [%s]' % lfc.sstrerror(errcode))
    else:
        for i in list:
            if i.errcode in [0, errno.ENOENT, errno.EINVAL]:
                if not nfiles.has_key(i.guid):
                    checksum = None
                    if i.csumvalue not in [None, '', 0]:
                        if i.csumtype == 'MD':
                            checksum = 'md5:%s' % i.csumvalue
                        elif i.csumtype == 'AD':
                            checksum = 'ad:%s' % i.csumvalue
                    nfiles[i.guid] = {'lfn': files[i.guid],
                                      'fsize': i.filesize,
                                      'surls': [],
                                      'checksum': checksum}
                if i.errcode == 0:
                    nfiles[i.guid]['surls'].append(i.sfn)
            else:
                nfiles[i.guid] = LFCFileCatalogException('Failed looking up file on LFC [errcode: %s]' % i.errcode)
    disconnect()
    return nfiles


def test():

    import commands
    host = 'lfc1.ndgf.org' # 'grid.tsl.uu.se'

    guid = commands.getoutput('uuidgen').strip()
    lfn = 'test.lfn.%s' % guid
    surl = 'srm://test.host/pnfs/test.host/data/test/%s' % lfn
    dsn = 'user.mruser.field3.field4.TEST.field6'
    fsize = 1234
    checksum = 'ad:430fc0a0'
    archival = 'P'

    input = {guid: {'lfn': lfn,
                    'surl': surl,
                    'dsn': dsn,
                    'fsize': fsize,
                    'checksum': checksum,
                    'archival': archival}}

    print 'registering %s' % input

    result = bulkRegisterFiles(host, input)
    for guid in result:
        if isinstance(result[guid], LFCFileCatalogException):
            print 'LFC exception: %s' % result[guid]

    result = bulkFindReplicas(host, {guid: lfn})
    for guid in result:
        if isinstance(result[guid], LFCFileCatalogException):
            print 'LFC exception: %s' % result[guid]
        else:
            print guid, ':', result[guid]
        

if __name__ == '__main__':
    test()
    
