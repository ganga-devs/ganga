
# Change LogicalFile to be a wrapper for the DiracFile,
# make sure DiracFile replicates required functionality and add any possible translation layer here

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem
from GangaDirac.Lib.Files.DiracFile import DiracFile
from Ganga.Utility.logging                    import getLogger
logger      = getLogger()

class LogicalFile(DiracFile):
    #  Logical File schema
    #  Observing the 'old' 1.0 schema whilst preserving backwards compatability with the fact that we're translating the object into a DiracFile in this case
    _schema = Schema(Version(1,0), { 'name'      : SimpleItem(defvalue="",doc='the LFN filename a LogicalFile is constructed with' ),
                                     'namePattern'   : SimpleItem(defvalue="",doc='pattern of the file name',transient=1),
                                     'localDir'      : SimpleItem(defvalue=None,copyable=1,typelist=['str','type(None)'],
                                                                  doc='local dir where the file is stored, used from get and put methods',transient=1),
                                     'remoteDir'     : SimpleItem(defvalue="",doc='remote directory where the LFN is to be placed in the dirac base directory by the put method.',transient=1),
                                     'locations'     : SimpleItem(defvalue=[],copyable=1,typelist=['str'],sequence=1,
                                                                  doc="list of SE locations where the outputfiles are uploaded",transient=1),
                                     'compressed'    : SimpleItem(defvalue=False,typelist=['bool'],protected=0,
                                                                  doc='wheather the output file should be compressed before sending somewhere',transient=1),
                                     'lfn'           : SimpleItem(defvalue='',copyable=1,typelist=['str'],
                                                                  doc='return the logical file name/set the logical file name to use if not '\
                                                                      'using wildcards in namePattern',transient=1),
                                     'guid'          : SimpleItem(defvalue='',copyable=1,typelist=['str'],
                                                                  doc='return the GUID/set the GUID to use if not using wildcards in the namePattern.',transient=1),
                                     'subfiles'      : ComponentItem(category='gangafiles',defvalue=[], hidden=1, sequence=1, copyable=0,
                                                                     typelist=['GangaDirac.Lib.Files.DiracFile'],
                                                                     doc="collected files from the wildcard namePattern",transient=1),
                                     'failureReason' : SimpleItem(defvalue="", protected=1, copyable=0, doc='reason for the upload failure', transient=1)
                                     })
    _name = "LogicalFile"

#TODO:  Add warning to User NOT to create these objects themselves and that they should
#       only be used for backwards compatability to load old jobs

    def __init__( self, name="" ):

        super( LogicalFile, self ).__init__( lfn=name )

        self.name = name

        logger.warning( "!!! LogicalFile has been deprecated, this is now just a wrapper to the DiracFile object" )
        logger.warning( "!!! Please update your scripts before LogicalFile is removed" )

        self._setLFNnamePattern( _lfn=self.name, _namePattern='' )


    def __setstate__(self, dict ):
        super( LogicalFile, self).__setstate__( dict )
        self._setLFNnamePattern( _lfn=self.name, _namePattern='' )

    def __construct__( self, args):

        if len(args) >= 1:
            self.name = args[0]

        self._setLFNnamePattern( lfn=self.name, namePattern='' )

        if (len(args) != 1) or (type(args[0]) is not type('')):
            super( LogicalFile, self ).__construct__(args)
        else:
            self.name = strip_filename(args[0])

    def __setattr__( self, name, value ):

        if name == "name":
            self.name = value
            self.lfn = value
            import os.path
            self.namePattern = os.path.basename( value )
            self.remoteDir = os.path.dirname( value )
        super( LogicalFile, self ).__setattr__( name, value )

    def _attribute_filter__set__(self, obj_type, attrib_name):
        if attrib_name == "name":
            self._setLFNnamePattern( lfn=self.name, namePattern='' )
        return super( LogicalFile, self )._attribute_filter__set__( obj_type, attrib_name )

