from .exceptions import CredentialsError

from Ganga.GPIDev.Base.Proxy import stripProxy

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from datetime import timedelta
import os
from abc import ABCMeta, abstractmethod

class ICredentialInfo(object):
    """
    The interface for all credential types.
    Each object covers one credential file exactly.
    The proxy/token is central to the object and all inforamtion is gathered from there.
    Everything is cached internally
    
    These are only created by the store and should not be persisted.
    """
    __metaclass__ = ABCMeta

    def __init__(self, requirements, check_file = False, create = False):
        """
        Args:
            requirements (ICredentialRequirement): An object specifying the requirements
            check_file: Raise an exception if the file does not exist
            create: Create the credential file
        
        Raises:
            IOError: If told to wrap a non-existent file
            CredentialsError: If this object cannot satisfy ``requirements``
        """
        super(ICredentialInfo, self).__init__()
        
        if not requirements.location:
            raise ValueError('requirements.location must be set')
        
        #First choose the filename to use
        self.initialRequirements = requirements #Store the requirements that the object was created with. Used for renewal
        
        if check_file:
            logger.info("Trying to wrap {path}".format(path=self.location))
            if not os.path.exists(self.location):
                raise IOError("Proxy file {path} not found".format(self.location))
            logger.info("Wrapping existing file %s", self.location)
            
        if create:
            logger.info("Making a new one")
            self.renew()
            
        # If the proxy object does not satisfy the requirements then abort the construction
        if not self.checkRequirements(requirements):
            raise CredentialsError('Proxy object cannot satisfy its own requirements')
    
    def __str__(self):
        return '{class_name} at {file_path}'.format(class_name=type(self).__name__, file_path=self.location)
    
    @property
    def location(self):
        return self.initialRequirements.location
    
    @abstractmethod
    def renew(self):
        pass

    def isValid(self):
        #TODO We should check that there's more than some minimum time left on the proxy
        return self.timeLeftInSeconds() > 0
    
    def timeLeft(self):
        """
        Returns the time left in a human readable form
        """
        return timedelta(seconds=self.timeLeftInSeconds())
    
    @abstractmethod
    def timeLeftInSeconds(self):
        pass
    
    def checkRequirements(self, query):
        """
        Args:
            query (ICredentialRequirement): The requirements to check ourself against
        
        Checks all requirements.
        
        Returns:
            ``True`` if we meet all requirements
            ``False`` if even one requirement is not met or if the credential is not valid
        """
        if not self.isValid():
            logger.info("Credential {file} is not valid.".format(file=self.location))
            return False
        return all(self.checkRequirement(query, requirementName) for requirementName in stripProxy(query)._schema.datadict)

    def checkRequirement(self, query, requirementName):
        """
        Args:
            query (ICredentialRequirement): 
            requirementName (str): The requirement attribute to check
        
        Returns:
            ``True`` if ``self`` matches ``query``'s ``requirement``.
        """
        requirementValue = getattr(query, requirementName)
        logger.info('{name}: \t{cred} \t{requested}'.format(name=requirementName, cred=getattr(self,requirementName), requested=requirementValue))
        if requirementValue is None:
            #If this requirementName is unspecified then ignore it
            return True
        return getattr(self,requirementName) == requirementValue
