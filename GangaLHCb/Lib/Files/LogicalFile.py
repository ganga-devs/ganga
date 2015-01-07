
# Change LogicalFile to be a wrapper for the DiracFile,
# make sure DiracFile replicates required functionality and add any possible translation layer here

from Ganga.GPIDev.Schema import Schema, Version
from GangaDirac.Lib.Files.DiracFile import DiracFile

class LogicalFile(DiracFile):
    _schema = Schema(Version(1,1), { } )#DiracFile._schema
    _name = "LogicalFile"

#TODO:  Add warning to User NOT to create these objects themselves and that they should
#       only be used for backwards compatability to load old jobs

