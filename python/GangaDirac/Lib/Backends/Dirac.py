#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''The Ganga backendhandler for the Dirac system. '''

from GangaDirac.Lib.Backends.DiracBase import DiracBase

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class Dirac(DiracBase):

    _name = 'Dirac'
    _category = 'backends'
    _schema = DiracBase._schema.inherit_copy()
    _exportmethods = DiracBase._exportmethods[:]
    _packed_input_sandbox = DiracBase._packed_input_sandbox
    __doc__ = DiracBase.__doc__

    def __init__(self):
        super(Dirac, self).__init__()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
