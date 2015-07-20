from Ganga.GPIDev.Base.Objects import GangaObject, ObjectMetaABC
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem

from abc import abstractmethod, abstractproperty


class ICredentialRequirement(GangaObject):
    """
    Requirements objects specify what is needed by a particular object in Ganga
    
    All subclasses must specify ``_infoClass`` as well as all abstract methods
    """
    __metaclass__ = ObjectMetaABC
    
    _schema = Schema(Version(1, 0), {
        'location': SimpleItem(defvalue=None, typelist=['str', 'None'], doc="Path to the proxy file"),
    })
    
    _category = "credentials2"
    _name = "ICredentialRequirement"
    _hidden = 1

    _exportmethods = ['encoded', 'isEmpty']
    
    _infoClass = None # This attribute refers to the ICredentialInfo subclass which can satisfy the requirements
    
    @abstractmethod
    def encoded(self):
        """
        Return a string which encoded all the requirements.
        This string will be appended to the filename so it should be filesystem-friendly.
        """
        pass
    
    @abstractmethod
    def set_defaults_from_config(self):
        """
        Fill this object's parameters with values from the user's default config
        """
        pass
    
    @abstractmethod
    def default_location(self):
        """
        Returns the default location for the credential file.
        This is the location that most tools will look for the file or where the file is created without specifying anything.
        TODO Should this return a list?
        TODO Should this be in ICredentialInfo?
        """
        pass

    @abstractmethod
    def is_empty(self):
        """
        Returns:
        ``True`` if no explicit requirements were specified (ignoring the ``location``)
        """
        pass

    def __hash__(self):
        return hash(self.encoded())

    def __eq__(self, other):
        return self.encoded() == other.encoded()
