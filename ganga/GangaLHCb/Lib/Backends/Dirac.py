from GangaDirac.Lib.Files.DiracFile import DiracFile
from GangaDirac.Lib.Backends.DiracBase import DiracBase
from GangaDirac.Lib.Backends.DiracUtils import result_ok
from GangaCore.GPIDev.Schema import Schema, Version, ComponentItem
from GangaCore.Core.exceptions import GangaException, BackendError
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
from GangaCore.GPIDev.Base.Proxy import GPIProxyObjectFactory
from GangaDirac.Lib.Utilities.DiracUtilities import execute
import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()
from GangaCore.GPIDev.Credentials              import require_credential


class Dirac(DiracBase):
    _schema = DiracBase._schema.inherit_copy()

    _schema.version.major += 0
    _schema.version.minor += 0
    _exportmethods = DiracBase._exportmethods[:]
    _exportmethods += ['checkSites', 'checkTier1s']
    _packed_input_sandbox = True
    _category = "backends"
    _name = 'Dirac'
    __doc__ = DiracBase.__doc__

    def _addition_sandbox_content(self, subjobconfig):
        input_sandbox = []
        j = self.getJobObject()
        if hasattr(j.inputfiles, 'get'):
            for f in j.inputfiles.get(DiracFile):
                if f.lfn == '':
                    raise GangaException(
                        'Can not add the lfn of of the DiracFile with name pattern: %s as the lfn property has not been set.' % f.namePattern)
                else:
                    input_sandbox.append('LFN:' + f.lfn)
        return input_sandbox

    def _setup_subjob_dataset(self, dataset):
        return LHCbDataset(files=[DiracFile(lfn=f) for f in dataset])

    @require_credential
    def checkSites(self):
        cmd = 'checkSites()'
        result = execute(cmd, cred_req=self.credential_requirements)
        if not result_ok(result):
            logger.warning('Could not obtain site info: %s' % str(result))
            return
        return result.get('Value', {})

    @require_credential
    def checkTier1s(self):
        cmd = 'checkTier1s()'
        result = execute(cmd, cred_req=self.credential_requirements)
        if not result_ok(result):
            logger.warning('Could not obtain Tier-1 info: %s' % str(result))
            return
        return result.get('Value', {})

    def getOutputDataLFNs(self):
        """Get a list of outputdata that has been uploaded by Dirac. Excludes
        the outputsandbox if it is there."""
        lfns = super(Dirac, self).getOutputDataLFNs()
        ds = LHCbDataset()
        for f in lfns:
            ds.files.append(DiracFile(lfn=f))
        return GPIProxyObjectFactory(ds)

    def getOutputData(self, outputDir=None, names=None, force=False):
        """Retrieve data stored on SE to outputDir (default=job output workspace).
        If names=None, then all outputdata is downloaded otherwise names should
        be a list of files to download. If force is True then download performed
        even if data already exists."""
        downloaded_files = super(Dirac, self).getOutputData(outputDir, names, force)
        ds = LHCbDataset()
        for f in downloaded_files:
            ds.files.append(DiracFile(lfn=f))
        return GPIProxyObjectFactory(ds)

def getLFNMetadata(lfns, credential_requirements=None):
    '''Return the file metadata for a given LFN or list of LFNs'''
    result = execute('getFileMetadata(%s)' % lfns, cred_req = credential_requirements )
    returnDict = {}
    if 'Successful' in result.keys():
        for _lfn in result['Successful'].keys():
            returnDict[_lfn] = {}
            returnDict[_lfn]['Luminosity'] = result['Successful'][_lfn]['Luminosity']
            returnDict[_lfn]['EventStat'] = result['Successful'][_lfn]['EventStat']
            returnDict[_lfn]['RunNumber'] = result['Successful'][_lfn]['RunNumber']

    return returnDict

from GangaCore.Runtime.GPIexport import exportToGPI
exportToGPI('getLFNMetadata', getLFNMetadata, 'Functions')
