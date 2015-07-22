from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version
from Ganga.GPIDev.Base.Proxy import stripProxy

from .exceptions import CredentialsError

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


class CredentialStore(GangaObject):
    """
    The central management for all credentials
    
    It is not intended to store the credential objects between sessions,
    rather it will search the filesystem or create new credential files.
    
    A single instance of this class makes most sense and should be created in the bootstrap and exported.
    """

    _schema = Schema(Version(1, 0), {})
                                                                                
    _category = "credentials2"
    _name = "CredentialStore"
    _hidden = 1  # This class is hidden since we want a 'singleton' created in the bootstrap

    _exportmethods = ['get', '__iter__']

    def __init__(self):
        super(CredentialStore, self).__init__()
        self.credentialList = set()
    
    def add(self, credential_object):
        """
        Adds ``credential_object`` to the store.
        
        Args:
            credential_object (ICredentialInfo): The object to add to the store
        
        Returns:
            The object passed in
        """

        self.credentialList.add(credential_object)
        return credential_object
    
    def create(self, query, create=False, check_file=False):
        """
        Create an ``ICredentialInfo`` for the query.
        
        Args:
            query (ICredentialRequirement): 
            check_file: Raise an exception if the file does not exist
            create: Create the credential file
        
        Returns:
            The newly created ICredentialInfo object
        """

        return stripProxy(query)._infoClass(query, create=create, check_file=check_file)
    
    def remove(self, credential_object):
        """
        Args:
            credential_object (ICredentialInfo):
        """

        self.credentialList.remove(credential_object)
    
    def __iter__(self):
        """Allow iterating over the store directly"""
        # yield from self.credentialList #In Python 3.3
        return iter(self.credentialList)
    
    def get(self, query):
        """
        This function will try quite hard to create the credential if it doesn't exist yet.
        If you want to just search the database, use ``match()``.
        
        Args:
            query (ICredentialRequirement):
        
        Returns:
            A single ICredentialInfo object which matches the requirements
        
        Raises:
            CredentialsError: If it could not provide a credential
        """

        query = stripProxy(query)

        if not query.location and query.is_empty():  # If there's nothing there at all
            query.set_defaults_from_config()
        
        match = self.match(query)
        if match:
            return match
        
        if query.is_empty():
            query.set_defaults_from_config()
        
        # By this point ``query`` definitely contains some options (user-specified or default) but might not have a ``location``
        
        # Assemble the locations to try
        # The order of this list matters as the last element is the name used to _create_ the proxy file if it cannot be found on disk
        if query.location:
            location_list = [query.location] #Just use the specified one if it exists
        else:
            location_list = [query.default_location(), ':'.join([query.default_location(), query.encoded()])] #Otherwise try some default places
        
        # For each location, try wrapping the file on disk
        for location in location_list:
            query.location = location
            try:
                cred = self.create(query, check_file = True)
            except IOError as e:
                logger.info(e.strerror)
            except CredentialsError as e:
                logger.info(str(e))
            else:
                return self.add(cred)
        
        # By this point both the location and the options are set
        
        cred = self.create(query, create=True)
        return self.add(cred)
    
    def get_all_matching_type(self, query):
        """
        Returns all ``ICredentialInfo`` with the type that matches the query
        
        Args:
        query (ICredentialRequirement): 
        """

        return (cred for cred in self.credentialList if type(cred) == stripProxy(query)._infoClass)
    
    def matches(self, query):
        """
        Search the credentials in the store for all matches. They must match every condition exactly.
        
        Args:
            query (ICredentialRequirement): 
           
        Returns:
            iterator: An iterator of all matching objects
        """

        return (cred for cred in self.get_all_matching_type(query) if cred.check_requirements(query))
    
    def match(self, query):
        """
        Returns a single match from the store
        
        Args:
            query (ICredentialRequirement):
        
        Returns:
            ICredentialInfo: A single credential object. If more than one is found, the first is returned
        """

        matches = list(self.matches(query))
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            logger.info("More than one match...")
            # If we have a specific object and a general one. Then we ask for a general one, what should we do.
            # Does it matter since they've only asked for a general proxy? What are the use cases?
            return matches[0]  # TODO For now just return the first one... Though perhaps we should merge them or something?
        return None

# This is a global 'singleton'
credential_store = CredentialStore()
