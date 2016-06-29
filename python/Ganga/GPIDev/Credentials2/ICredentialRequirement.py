from abc import abstractmethod

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version


class ICredentialRequirement(GangaObject):
    """
    Requirements objects specify what is needed by a particular object in Ganga

    All subclasses must specify ``info_class`` as well as all abstract methods
    """

    _schema = Schema(Version(1, 0))

    _category = 'CredentialRequirement'
    _name = 'ICredentialRequirement'
    _hidden = 1

    info_class = None  # This attribute refers to the ICredentialInfo subclass which can satisfy the requirements

    def __init__(self, **kwargs):
        super(ICredentialRequirement, self).__init__()
        for key, value in kwargs.items():
            if key not in self._schema.allItemNames():
                raise Exception()
            setattr(self, key, value)

    @abstractmethod
    def encoded(self):
        """
        Return a string which encoded all the requirements.
        This string will be appended to the filename so it should be filesystem-friendly.
        """
        pass

    def __str__(self):
        items = ((name, str(getattr(self, name))) for name in self._schema.allItemNames())
        arg_strings = ('='.join(arg) for arg in items)
        arg_string = ', '.join(arg_strings)
        return '{name}({args})'.format(name=self.__class__.__name__, args=arg_string)

    def __hash__(self):
        return hash(self.__class__.__name__ + self.encoded())

    def __eq__(self, other):
        if other is None:
            return False
        return type(self) is type(other) and self.encoded() == other.encoded()
