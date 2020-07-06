# ******************** Imports ******************** #

import os
import uuid
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui import app, db
from GangaGUI.gui.models import User
from GangaCore.GPIDev.Schema import SimpleItem
from GangaCore.GPIDev.Credentials.CredentialStore import CredentialStore
from GangaCore.GPIDev.Adapters.ICredentialInfo import ICredentialInfo
from GangaCore.GPIDev.Adapters.ICredentialRequirement import ICredentialRequirement
from datetime import datetime, timedelta

# ******************** Global Variables ******************** #

currentdir = os.path.dirname(os.path.abspath(__file__))
token = None

# ******************** Classes to Mock Credentials ******************** #

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

    def check_requirements(self, query):
        return True

    def exists(self):
        return self.does_exist


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


# ******************** Test Class ******************** #

# Credential Store API Tests
class TestGangaGUICredentialStoreAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUICredentialStoreAPI, self).setUp(extra_opts=[])

        # App config and database creation
        app.config["TESTING"] = True
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(currentdir, "gui_test.sqlite")
        db.create_all()

        # Temp user for the test
        tempuser = User(public_id=str(uuid.uuid4()), user="GangaGUITestUser", role="Admin")
        tempuser.store_password_hash("testpassword")
        db.session.add(tempuser)
        db.session.commit()

        # Flask test client
        self.app = app.test_client()

        # Generate token for requests
        global token
        res = self.app.post("/token", data={"user": "GangaGUITestUser", "password": "testpassword"})
        token = res.json["token"]

    # Credential Store API - GET Method - Get list of all the credentials and their info
    def test_GET_method(self):
        from GangaCore.GPI import credential_store

        # Create mock credentials
        credential_store.create(FakeCred())
        assert len(credential_store) == 1

        # Create mock credentials
        credential_store.create(FakeCred(vo='some_other_vo'))
        assert len(credential_store) == 2

        # GET request
        res = self.app.get(f"/api/credential_store", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(res.json) == len(credential_store))

    # Credential Store API - PUT Method - Renew all the credentials in the credential sotre
    def test_PUT_method(self):
        from GangaCore.GPI import credential_store

        # Create mock credentials
        credential_store.create(FakeCred())
        assert len(credential_store) == 1

        # Create mock credentials
        credential_store.create(FakeCred(vo='some_other_vo'))
        assert len(credential_store) == 2

        # PUT request
        res = self.app.put(f"/api/credential_store/renew", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)

    # Tear down
    def tearDown(self):
        super(TestGangaGUICredentialStoreAPI, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))
