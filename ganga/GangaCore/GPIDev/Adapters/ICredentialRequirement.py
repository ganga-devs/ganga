from abc import abstractmethod

from GangaCore.Core.exceptions import GangaAttributeError
from GangaCore.GPIDev.Base.Objects import GangaObject
from GangaCore.GPIDev.Schema import Schema, Version


class ICredentialRequirement(GangaObject):
    """
    Requirements objects specify what is needed by a particular object in Ganga

    All subclasses must specify ``info_class`` as well as all abstract methods
    """

    _schema = Schema(Version(1, 0))

    _category = 'CredentialRequirement'
    _name = 'ICredentialRequirement'
    _hidden = 1

    __slots__ = list()

    # This attribute refers to the ICredentialInfo subclass which can satisfy the requirements
    info_class = None  # type: Type[ICredentialInfo]

    def __init__(self, **kwargs):
        super(ICredentialRequirement, self).__init__()
        for key, value in kwargs.items():
            if key not in self._schema.allItemNames():
                raise GangaAttributeError('{class_name} does not have attribute called "{attr}"'.format(class_name=self.__class__.__name__, attr=key))
            setattr(self, key, value)

    @abstractmethod
    def encoded(self):
        """
        Return a string which encoded all the requirements.
        This string will be appended to the filename so it should be filesystem-friendly.
        """
        pass

    def __str__(self):
        """ Returns the repr as defined within __repr__ """
        return repr(self)

    def __repr__(self):
        """ This is a custom strinf repr of the class which can regenerate it on the IPython prompt. FIXME: This should rely on GangaObject. """
        items = ((name, getattr(self, name)) for name in self._schema.allItemNames())  # Name,value pairs for all schema items
        explicit_items = ((name, value) for name, value in items if value)  # Filter out any default values
        item_strings = ((name, repr(value)) for name, value in explicit_items)  # Stringify the values
        arg_strings = ('='.join(arg) for arg in item_strings)  # Make "name=value" strings
        arg_string = ', '.join(arg_strings)  # Make a full "a=1, b=2" string
        return '{name}({args})'.format(name=self.__class__.__name__, args=arg_string)

    def __hash__(self):
        """ Returns a hash of the name of the credential requirement and the location of the requirement on disk with additional encoding """
        return hash(self.__class__.__name__ + self.encoded())

    def __eq__(self, other):
        """ This compares 2 Credential Requirements based upon additional encoding of the credential properties """
        if other is None:
            return False
        return type(self) is type(other) and self.encoded() == other.encoded()
