from datetime import datetime, timedelta

import pytest

from Ganga.GPIDev.Schema import SimpleItem
from Ganga.GPIDev.Credentials2.CredentialStore import CredentialStore
from Ganga.GPIDev.Credentials2.ICredentialInfo import ICredentialInfo
from Ganga.GPIDev.Credentials2.ICredentialRequirement import ICredentialRequirement


class FakeCredInfo(ICredentialInfo):
    def __init__(self, requirements, check_file=False, create=False):

        self.does_exist = False

        super(FakeCredInfo, self).__init__(requirements, check_file, create)

    def create(self):
        self.does_exist = True

    def destroy(self):
        self.does_exist = False

    @property
    def vo(self):
        if self.does_exist:
            return self.initial_requirements.vo
        return None

    @property
    def role(self):
        if self.does_exist:
            return self.initial_requirements.role
        return None

    def expiry_time(self):
        if self.does_exist:
            return datetime.now() + timedelta(days=1)

        datetime.now()

    def default_location(self):
        return '/tmp/some_fake_file'


class FakeCred(ICredentialRequirement):
    """
    An object specifying the requirements of a VOMS proxy file for testing
    """
    _schema = ICredentialRequirement._schema.inherit_copy()
    _schema.datadict['vo'] = SimpleItem(defvalue=None, typelist=[str, None])
    _schema.datadict['role'] = SimpleItem(defvalue=None, typelist=[str, None])

    _category = 'CredentialRequirement'
    _name = 'TestCred'

    info_class = FakeCredInfo

    def __init__(self, **kwargs):
        super(FakeCred, self).__init__(**kwargs)

    def encoded(self):
        return ':'.join(requirement for requirement in [self.vo, self.role] if requirement)

    def is_empty(self):
        return not (self.vo or self.role)


def test_get_missing_credential():
    """
    Check that asking for a non-existent credential fails
    """
    store = CredentialStore()
    req = FakeCred()

    with pytest.raises(KeyError):
        _ = store[req]


def test_create_remove():
    """
    Ensure that credentials can be created and removed
    """
    store = CredentialStore()
    req = FakeCred()

    info = store.create(req)
    assert info is store[req]

    store.remove(info)

    with pytest.raises(KeyError):
        store.remove(info)

    with pytest.raises(KeyError):
        _ = store[req]


def test_get():
    """
    Make sure that the ``get()`` method works as expected
    """
    store = CredentialStore()
    req = FakeCred()

    assert store.get(req) is None
    info = store.create(req)
    assert store.get(req) is info


def test_len():
    """
    Ensure that the credential count is updated correctly
    """
    store = CredentialStore()

    store.create(FakeCred())
    assert len(store) == 1

    store.create(FakeCred(vo='some_other_vo'))
    assert len(store) == 2

    store.create(FakeCred())
    assert len(store) == 2, 'Adding a repeated requirement should not increase the size of the store'

    assert len(store.get_all_matching_type(FakeCred)) == 2

    store.remove(store[FakeCred()])
    assert len(store) == 1

    store.clear()
    assert len(store) == 0
