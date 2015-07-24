from datetime import timedelta
import json

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version
from Ganga.GPIDev.Base.Proxy import stripProxy

from .exceptions import CredentialsError
from .ICredentialRequirement import ICredentialRequirement

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

    _exportmethods = ['__getitem__', '__iter__', 'renew', 'create']

    def __init__(self):
        super(CredentialStore, self).__init__()
        self.credentials = set()
    
    def create(self, query, create=True, check_file=False):
        """
        Create an ``ICredentialInfo`` for the query.
        
        Args:
            query (ICredentialRequirement): 
            check_file: Raise an exception if the file does not exist
            create: Create the credential file
        
        Returns:
            The newly created ICredentialInfo object
        """

        cred = stripProxy(query)._infoClass(query, check_file=check_file, create=create)
        self.credentials.add(cred)
        logger.info('returning %s', cred)
        return cred
    
    def remove(self, credential_object):
        """
        Args:
            credential_object (ICredentialInfo):
        """

        self.credentials.remove(credential_object)
    
    def __iter__(self):
        """Allow iterating over the store directly"""
        # yield from self.credentialList #In Python 3.3
        return iter(self.credentials)

    def __getitem__(self, query):
        """
        This function will try quite hard to find and wrap any missing credential
        but will never create a new file on disk

        Args:
            query (ICredentialRequirement):

        Returns:
            A single ICredentialInfo object which matches the requirements

        Raises:
            CredentialsError: If it could not provide a credential
        """
        query = stripProxy(query)

        if not isinstance(query, ICredentialRequirement):
            raise TypeError('Credential store index should be of type ICredentialRequirement')

        match = self.match(query)
        if match:
            logger.info('found match %s', match)
            return match

        # Assemble the locations to try
        if query.location:
            location_list = [query.location]  # Just use the specified one if it exists
        else:  # Otherwise try some default places
            location_list = [query.default_location(), ':'.join([query.default_location(), query.encoded()])]

        # For each location, try wrapping the file on disk
        for location in location_list:
            query.location = location
            try:
                cred = self.create(query, create=False, check_file=True)
            except IOError as e:
                logger.info(e.strerror)
            except CredentialsError as e:
                logger.info(str(e))
            else:
                logger.info('made match %s', cred)
                self.credentials.add(cred)
                return cred

        raise KeyError('Matching credential not found in store.')
    
    def get_all_matching_type(self, query):
        """
        Returns all ``ICredentialInfo`` with the type that matches the query
        
        Args:
            query (ICredentialRequirement):
        """

        return (cred for cred in self.credentials if type(cred) == stripProxy(query)._infoClass)
    
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

    def renew(self):
        """
        Renew all credentails which are invalid or will expire soon
        """
        for cred in self.credentials:
            if not cred.is_valid() or cred.time_left() < timedelta(hours=1):
                cred.renew()

# This is a global 'singleton'
credential_store = CredentialStore()
