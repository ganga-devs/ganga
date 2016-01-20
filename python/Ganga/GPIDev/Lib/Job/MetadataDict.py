from Ganga.GPIDev.Base import GangaObject
from Ganga.Core.exceptions import GangaAttributeError
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.GPIDev.Base.Proxy import addProxy


class MetadataDict(GangaObject):

    '''MetadataDict class

    Class that represents the dictionary of metadata.
    '''
    _schema = Schema(Version(1, 0), {
        'data': SimpleItem(defvalue={}, doc='dict data', hidden=1, protected=1)
    })
    _name = 'MetadataDict'
    _category = 'metadata'
    _exportmethods = ['__getitem__']  # ,'__str__']

    def __init__(self):
        super(MetadataDict, self).__init__()

# def __str__(self):
# return str(self.data)

    def __getitem__(self, key):
        import copy
        return addProxy(copy.deepcopy(self.data[key]))

    def __setitem__(self, key, value):
        from Ganga.GPIDev.Lib.Job.Job import Job
        if key in Job._schema.datadict.keys():
            raise GangaAttributeError(
                '\'%s\' is a reserved key name and cannot be used in the metadata' % key)
        if not isinstance(key, str):
            raise GangaAttributeError(
                'Metadata key must be of type \'str\' not %s' % type(key))
        if isinstance(value, GangaObject):
            raise GangaAttributeError(
                'Metadata doesn\'t support nesting of GangaObjects at the moment')
# if type(value) is not type(''):
##             raise GangaAttributeError('Metadata only supports string values at the moment')
# if type(value) is list or type(value) is tuple or type(value) is dict:
##             raise GangaAttributeError('Metadata doesn\'t support nesting data structures at the moment, values of type \'list\', \'tuple\' or \'dict\' are forbidden')

        self.data[key] = value
        self._setDirty()

    def update(self, dict):

        # this way pick up the checking for free
        for key, value in dict.iteritems():
            self.__setitem__(key, value)
#        self.data.update(dict)

    def printSummaryTree(self, level=0, verbosity_level=0, whitespace_marker='', out=None, selection='', interactive=False):
        """If this method is overridden, the following should be noted:

        level: the hierachy level we are currently at in the object tree.
        verbosity_level: How verbose the print should be. Currently this is always 0.
        whitespace_marker: If printing on multiple lines, this allows the default indentation to be replicated.
                           The first line should never use this, as the substitution is 'name = %s' % printSummaryTree()
        out: An output stream to print to. The last line of output should be printed without a newline.'
        selection: See VPrinter for an explaintion of this.
        """
        if len(self.data) == 0:
            out.write('{}\n')
            return
        out.write('{\n')
        for key, value in self.data.iteritems():
            out.write(whitespace_marker + '     ' + str(key) + ' = ' + str(value) + '\n')
        out.write(whitespace_marker + '    }\n')
