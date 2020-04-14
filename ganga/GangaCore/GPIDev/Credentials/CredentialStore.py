

import time
import collections
from datetime import timedelta

import GangaCore.Utility.logging
from GangaCore.GPIDev.Base.Objects import GangaObject
from GangaCore.GPIDev.Base.Proxy import export
from GangaCore.GPIDev.Schema import Schema, Version

from GangaCore.Core.exceptions import CredentialsError, GangaKeyError, GangaTypeError
from GangaCore.GPIDev.Adapters.ICredentialRequirement import ICredentialRequirement

from GangaCore.Utility.Config import getConfig

logger = GangaCore.Utility.logging.getLogger()


class CredentialStore(GangaObject, collections.abc.Mapping):
    """
    The central management for all credentials

    It is not intended to store the credential objects between sessions,
    rather it will search the filesystem or create new credential files.

    Its interface is similar to a dictionary with ``ICredentialRequirement``
    objects as keys and ``ICredentialInfo`` objects as values.

    A single instance of this class makes most sense and should be created in the bootstrap and exported.


    E.g:

    To create a new Afs Token call::

        credential_store.create(AfsToken())

    To create a new Dirac Proxy call::

        credential_store.create(DiracProxy())


    To destroy a proxy which is already in the store call::

        credential_store[VomsProxy()].destroy()


    To get a summary of the available proxies already in the store simply type::

        credential_store
    """

    _schema = Schema(Version(1, 0), {})

    _category = 'credentials2'
    _hidden = 1  # This class is hidden since we want a 'singleton' created in the bootstrap

    retry_limit = 5
    enable_caching = True

    _last_clean = None
    _clean_delay = getConfig('Credentials')['CleanDelay']

    __slots__ = ('credentials',)

    def __init__(self):
        super(CredentialStore, self).__init__()
        self.credentials = set()

    @export
    def create(self, query, create=True, check_file=False):
        # type: (ICredentialRequirement, bool, bool) -> ICredentialInfo
        """
        Create an ``ICredentialInfo`` for the query.

        Args:
            query (ICredentialRequirement):
            check_file (bool): Raise an exception if the file does not exist
            create (bool): Create the credential file

        Returns:
            The newly created ICredentialInfo object
        """

        try:
            assert isinstance(query, ICredentialRequirement), "Error checking 'query'"
            assert isinstance(create, bool), "Error checking 'create'"
            assert isinstance(check_file, bool), "Error checking 'check_file'"
        except AssertionError as err:
            raise CredentialsError("Requirements to make a Credential are wrong. Please check your arguments. %s" % err)

        cred = query.info_class(query, check_file=check_file, create=create)
        self.credentials.add(cred)
        return cred

    def remove(self, credential_object):
        # type: (ICredentialInfo) -> None
        """
        Args:
            credential_object (ICredentialInfo):
        """

        self.credentials.remove(credential_object)

    @export
    def __str__(self, interactive=False):
        """
        This creates a table summary of all credentials known to the store
        """
        self.clean()
        headers = ['Type', 'Location', 'Valid', 'Time left']
        cred_info = [
                        [str(f) for f in (type(cred).__name__, cred.location, cred.is_valid(), cred.time_left())]  # Format each field as a string
                        for cred in self.credentials  # for each credential in the store
                    ]
        return self._create_table(headers, cred_info)

    @staticmethod
    def _create_table(headers, data):
        """
        Create a formatted table out of the headers and data
        Args:
            headers (List[str]): the strings for the table headers
            data (List[List[str]]): the body of the table

        Returns (str): a formatted string displaying the data
        """
        rows = [headers] + data
        # Get the length of the longest string in each column
        column_widths = [
            max(len(field) for field in column)
            for column in zip(*rows)
            ]

        def pad_row_strings(row, widths, filler=' '):
            # type: (List[str], List[int], str) -> List[str]
            """Add padding to each of ``row`` to equal the corresponding ``widths`` entry, padded with ``filler``"""
            return ['{field:{filler}<{width}}'.format(field=field[0], filler=filler, width=field[1]) for field in zip(row, widths)]
        padded_headers = pad_row_strings(headers, column_widths)
        padded_dividers = pad_row_strings([''] * len(column_widths), column_widths, filler='-')
        padded_data = [pad_row_strings(row, column_widths) for row in data]

        # Concatenate the field strings together
        def strings_to_row(strings, spacer='|'):
            # type: (List[str], str) -> str
            return ' {0} '.format(spacer).join(strings)
        header = strings_to_row(padded_headers)
        divider = strings_to_row(padded_dividers, spacer='+')
        body = '\n'.join(strings_to_row(padded_field_strings) for padded_field_strings in padded_data)

        return '\n'.join([header, divider, body])

    @export
    def __iter__(self):
        """Allow iterating over the store directly"""
        # yield from self.credentialList #In Python 3.3
        return iter(self.credentials)

    @export
    def __len__(self):
        """How many credentials are known about in the system"""
        return len(self.credentials)

    @export
    def __getitem__(self, query):
        # type: (ICredentialRequirement) -> ICredentialInfo
        """
        This function will try quite hard to find and wrap any
        missing credential but will never create a new file on disk.
        It should *always* be non-interactive so that it can be run
        on background threads without issue.

        Args:
            query (ICredentialRequirement):

        Returns:
            A single ICredentialInfo object which matches the requirements

        Raises:
            GangaKeyError: If it could not provide a credential
            GangaTypeError: If query is of the wrong type
        """

        self.clean()

        if not isinstance(query, ICredentialRequirement):
            raise GangaTypeError('Credential store query should be of type ICredentialRequirement')

        match = self.match(query)
        if match:
            return match

        try:
            cred = self.create(query, create=False, check_file=True)
        except IOError as err:
            logger.debug(err.strerror)
        except CredentialsError as err:
            logger.debug(str(err))
        else:
            self.credentials.add(cred)
            return cred

        raise GangaKeyError('Matching credential [{query}] not found in store.'.format(query=query))

    def get(self, query, default=None):
        # type: (ICredentialRequirement, Optional[ICredentialInfo]) -> Optional[ICredentialInfo]
        """
        Return the value for ``query`` if ``query`` is in the store, else default.
        If ``default`` is not given, it defaults to ``None``, so that this method never raises a ``KeyError``.

        Args:
            query (ICredentialRequirement):
            default (ICredentialInfo):

        Returns:
            A single ICredentialInfo object which matches the requirements or ``default``
        """

        try:
            assert isinstance(query, ICredentialRequirement), "Error checking 'query'"
            if default is not None:
                assert isinstance(default, ICredentialInfo), "Error checking 'default'"
        except AssertionError as err:
            raise CredentialsError("Requirements for get-ing a Credential are wrong. Please check your arguments. %s" % err)

        try:
            return self[query]
        except KeyError:
            return default

    def get_all_matching_type(self, query):
        # type: (ICredentialRequirement) -> Sequence[ICredentialInfo]
        """
        Returns all ``ICredentialInfo`` with the type that matches the query

        Args:
            query (ICredentialRequirement):

        Returns:
            list[ICredentialInfo]: An list of all matching objects
        """

        try:
            assert isinstance(query, ICredentialRequirement), "Error checking 'query'"
        except AssertionError as err:
            raise CredentialsError("Requirements for matching all Credential are wrong. Please check your arguments. %s" % err)

        return [cred for cred in self.credentials if isinstance(cred, query.info_class)]

    def matches(self, query):
        # type: (ICredentialRequirement) -> Sequence[ICredentialInfo]
        """
        Search the credentials in the store for all matches. They must match every condition exactly.

        Args:
            query (ICredentialRequirement):

        Returns:
            list[ICredentialInfo]: An list of all matching objects
        """

        try:
            assert isinstance(query, ICredentialRequirement), "Error checking 'query'"
        except AssertionError as err:
            raise CredentialsError("Requirements for matching any Credential are wrong. Please check your arguments. %s" % err)

        return [cred for cred in self.get_all_matching_type(query) if cred.check_requirements(query)]

    def match(self, query):
        # type: (ICredentialRequirement) -> ICredentialInfo
        """
        Returns a single match from the store

        Args:
            query (ICredentialRequirement):

        Returns:
            ICredentialInfo: A single credential object. If more than one is found, the first is returned
        """

        try:
            assert isinstance(query, ICredentialRequirement), "Error checking 'query'"
        except AssertionError as err:
            raise CredentialsError("Requirements for matching a Credential are wrong. Please check your arguments. %s" % err)

        matches = self.matches(query)
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            logger.debug('More than one match...')
            # If we have a specific object and a general one. Then we ask for a general one, what should we do.
            # Does it matter since they've only asked for a general proxy? What are the use cases?
            return matches[0]  # TODO For now just return the first one... Though perhaps we should merge them or something?
        return None

    @export
    def renew(self):
        # type: () -> None
        """
        Renew all credentials which are invalid or will expire soon.
        It also uses the entries in `needed_credentials` and adds and renews those
        TODO Should this function be standalone?
        """
        for cred in self.credentials:
            if not cred.is_valid() or cred.time_left() < timedelta(hours=1):
                cred.renew()
        for cred_req in needed_credentials - self.credentials:
            try:
                self[cred_req].renew()
            except KeyError:
                self.create(cred_req)

    @export
    def clear(self):
        # type: () -> None
        """
        Remove all credentials in the system (without destroying them)
        """
        self.credentials = set()

    def clean(self):
        # type: () -> None
        """
        Remove any credentials with missing files
        """
        this_time = time.time()
        if not CredentialStore._last_clean:
            CredentialStore._last_clean = this_time
            self.credentials = set(cred for cred in self.credentials if cred.exists())
        elif this_time - CredentialStore._last_clean > CredentialStore._clean_delay:
            self.credentials = set(cred for cred in self.credentials if cred.exists())
            CredentialStore._last_clean = this_time

# This is a global 'singleton'
credential_store = CredentialStore()

needed_credentials = set()  # type: Set[ICredentialRequirement]


def get_needed_credentials():
    # type: () -> Set[ICredentialRequirement]
    # Filter out any credentials which are valid
    now_valid_creds = set()  # type: Set[ICredentialRequirement]
    for cred_req in needed_credentials:
        cred = credential_store.get(cred_req)
        if cred and cred.is_valid():
            now_valid_creds.add(cred_req)

    # Remove the valid credentials from needed_credentials
    needed_credentials.difference_update(now_valid_creds)

    return needed_credentials


def shutdown():
    """
    Clean up after ourselves. This should be non-destructive with
    regards to anything stored on disk.
    """
    global needed_credentials
    credential_store.clear()
    needed_credentials = set()

