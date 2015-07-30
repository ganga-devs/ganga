from Ganga.GPIDev.Base.Objects import GangaObject, ObjectMetaABC
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem

from Ganga.GPIDev.Base.Proxy import addProxy

from abc import abstractmethod


class ICredentialRequirement(GangaObject):
    """
    Requirements objects specify what is needed by a particular object in Ganga
    
    All subclasses must specify ``_infoClass`` as well as all abstract methods
    """
    __metaclass__ = ObjectMetaABC
    
    _schema = Schema(Version(1, 0), {
        'location': SimpleItem(defvalue=None, typelist=['str', 'None'], doc="Path to the proxy file"),
    })
    
    _category = "CredentialRequirement"
    _name = "ICredentialRequirement"
    _hidden = 1

    _infoClass = None  # This attribute refers to the ICredentialInfo subclass which can satisfy the requirements

    def __init__(self):
        super(ICredentialRequirement, self).__init__()
    
    @abstractmethod
    def encoded(self):
        """
        Return a string which encoded all the requirements.
        This string will be appended to the filename so it should be filesystem-friendly.
        """
        pass

    @abstractmethod
    def default_location(self):
        """
        Returns the default location for the credential file.
        This is the location that most tools will look for the file
        or where the file is created without specifying anything.
        TODO Should this return a list?
        """
        pass

    @abstractmethod
    def is_empty(self):
        """
        Returns:
        ``True`` if no explicit requirements were specified (ignoring the ``location``)
        """
        pass

    def __str__(self):
        return str(addProxy(self)).replace(' ,\n', ',')

    def __hash__(self):
        return hash(self.encoded())

    def __eq__(self, other):
        if other is None:
            return False
        return self.encoded() == other.encoded()
