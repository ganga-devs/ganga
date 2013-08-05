from GangaDirac.Lib.Backends.DiracBase     import DiracBase
from GangaDirac.Lib.Backends.DiracUtils    import result_ok
from Ganga.GPIDev.Schema                   import Schema, Version, ComponentItem
from Ganga.Core                            import BackendError
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
from GangaLHCb.Lib.LHCbDataset.LogicalFile import LogicalFile
from Ganga.GPIDev.Base.Proxy               import GPIProxyObjectFactory
#from GangaDirac.Lib.Backends.DiracBase     import dirac_ganga_server
from GangaDirac.BOOT                       import dirac_ganga_server


class Dirac(DiracBase):
     _schema = DiracBase._schema.inherit_copy()
     _schema.datadict['inputSandboxLFNs'] = ComponentItem(category='datafiles', defvalue=[], sequence=1,
                                                          typelist=['GangaLHCb.Lib.LHCbDataset.LogicalFile.LogicalFile'],
                                                          doc='LFNs to be downloaded into the work dir on the grid node. Site '\
                                                          'matching is *not* performed on these files; they are downloaded.'\
                                                          'I.e., do not put prod data here')
     
     _schema.version.major += 0
     _schema.version.minor += 0
     _exportmethods = DiracBase._exportmethods[:]
     _exportmethods += ['checkSites','checkTier1s']
     _packed_input_sandbox = True
     _category = "backends"
     _name = 'Dirac'
     __doc__               = DiracBase.__doc__


     def _addition_sandbox_content(self,subjobconfig):
          input_sandbox = []
          for lfn in self.inputSandboxLFNs:
               from GangaLHCb.Lib.LHCbDataset.PhysicalFile import PhysicalFile
               if type(lfn) is PhysicalFile:
                    msg = 'Dirac.inputSandboxLFNs cannot contain a PhysicalFile.'
                    logger.error(msg)
                    raise BackendError('Dirac',msg)            
               input_sandbox.append('LFN:'+lfn.name)
          j = self.getJobObject()
          from GangaDirac.Lib.Files.DiracFile import DiracFile
          for f in j.inputfiles:
               if type(f) is DiracFile:
                    if f.lfn == '':
                         raise GangaException('Can not add the lfn of of the DiracFile with name pattern: %s as this property has not been set.' % f.namePattern)
                    else:
                         input_sandbox.append('LFN:' + f.lfn)
          return input_sandbox

     def _setup_subjob_dataset(self, dataset):
          return LHCbDataset(files=[LogicalFile(f) for f in dataset])
        

     def checkSites(self):
          cmd = 'checkSites()'
          result = dirac_ganga_server.execute(cmd)
#          result = Dirac.dirac_ganga_server.execute(cmd)
          if not result_ok(result):
               logger.warning('Could not obtain site info: %s' % str(result))
               return
          return result.get('Value',{})

     def checkTier1s(self):
          cmd = 'checkTier1s()'
          result = dirac_ganga_server.execute(cmd)
#          result = Dirac.dirac_ganga_server.execute(cmd)
          if not result_ok(result):
               logger.warning('Could not obtain Tier-1 info: %s' % str(result))
               return
          return result.get('Value',{})


     def getOutputDataLFNs(self):
          """Get a list of outputdata that has been uploaded by Dirac. Excludes
          the outputsandbox if it is there."""        
          lfns = super(Dirac,self).getOutputDataLFNs()
          ds = LHCbDataset()
          for f in lfns: ds.files.append(LogicalFile(f))
          return GPIProxyObjectFactory(ds)


     def getOutputData(self,dir=None,names=None, force=False):
          """Retrieve data stored on SE to dir (default=job output workspace).
          If names=None, then all outputdata is downloaded otherwise names should
          be a list of files to download. If force is True then download performed
          even if data already exists."""
          downloaded_files = super(Dirac,self).getOutputData(dir, names, force)
          ds = LHCbDataset()
          for f in downloaded_files: ds.files.append(LogicalFile(f))
          return GPIProxyObjectFactory(ds)
        
