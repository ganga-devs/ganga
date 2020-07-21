# ******************** Imports ******************** #

import os
import uuid
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui import app, db
from GangaGUI.gui.models import User

# ******************** Global Variables ******************** #

currentdir = os.path.dirname(os.path.abspath(__file__))
token = None


# ******************** Test Class ******************** #

# Job API Tests
class TestGangaGUIJobAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUIJobAPI, self).setUp(extra_opts=[])

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

    # Job API Test - GET Method, Single Job Info - Job ID out of index
    def test_GET_method_id_out_of_range(self):
        res = self.app.get(f"/api/job/1", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Job API Test - GET Method, Single Job Info - Job ID negative
    def test_GET_method_id_negative(self):
        res = self.app.get(f"/api/job/-1", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Job API Test - GET Method, Single Job Info - Job ID string
    def test_GET_method_id_string(self):
        res = self.app.get(f"/api/job/test", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Job API Test - GET Method, Single Job Info - Everything correct
    def test_GET_method(self):
        from GangaCore.GPI import Job

        # Create test job
        j = Job()
        j.name = "API Test Job"
        j.comment = "Test comment"

        # Make the GET request
        res = self.app.get(f"/api/job/{j.id}", headers={"X-Access-Token": token})

        # Response assertions
        self.assertTrue(res.status_code == 200)

        # Response data assertions
        self.assertTrue("application" in res.json.keys())
        self.assertTrue("backend" in res.json.keys())
        self.assertTrue("backend.actualCE" in res.json.keys())
        self.assertTrue("comment" in res.json.keys())
        self.assertTrue("fqid" in res.json.keys())
        self.assertTrue("id" in res.json.keys())
        self.assertTrue("name" in res.json.keys())
        self.assertTrue("subjob_statuses" in res.json.keys())
        self.assertTrue("subjobs" in res.json.keys())
        self.assertTrue(res.json["name"] == "API Test Job")
        self.assertTrue(int(res.json["id"]) == int(j.id))
        self.assertTrue(str(res.json["comment"]) == str(j.comment))

    # Job API Test - GET Method, Single Job Attribute Info - Job ID out of index
    def test_job_attribute_GET_method_id_out_of_index(self):
        res = self.app.get(f"/api/job/1/id", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Job API Test - GET Method, Single Job Attribute Info - Job ID negative
    def test_job_attribute_GET_method_id_negative(self):
        res = self.app.get(f"/api/job/-1/id", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Job API Test - GET Method, Single Job Attribute Info - Job ID string
    def test_job_attribute_GET_method_id_string(self):
        res = self.app.get(f"/api/job/test/id", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Job API Test - GET Method, Single Job Attribute Info - Job ID corrent but unsupported attribute
    def test_job_attribute_GET_method_unsupported_attribute(self):
        from GangaCore.GPI import Job

        # Create test job
        j = Job()
        j.name = "API Test Job"
        j.comment = "Test comment"

        # GET request
        res = self.app.get(f"/api/job/{j.id}/test", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Job API Test - GET Method, Single Job Attribute Info - Job ID correct and supported attribute
    def test_job_attribute_GET_method_supported_attribute(self):
        from GangaCore.GPI import Job

        # Create test job
        j = Job()
        j.name = "API Test Job"
        j.comment = "Test comment"

        # Supported attributes list
        supported_attributes = ["application", "backend", "do_auto_resubmit", "fqid", "id", "info", "inputdir",
                                "inputdata",
                                "inputfiles",
                                "master", "name", "outputdir", "outputdata",
                                "outputfiles", "parallel_submit", "splitter", "status", "time"]

        # For each attribute make a GET request and assert the response data
        for attribute in supported_attributes:
            res = self.app.get(f"/api/job/{j.id}/{attribute}", headers={"X-Access-Token": token})
            self.assertTrue(res.status_code == 200)
            self.assertTrue(attribute in res.json.keys())

    # Job API Test - POST Method, Create Job from Template - Template ID not provide in the request body
    def test_POST_method_template_id_not_given(self):
        res = self.app.post(f"/api/job/create", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Job API Test - POST Method, Create Job from Template - Template ID is not an int
    def test_POST_method_template_id_not_int(self):
        res = self.app.post(f"/api/job/create", headers={"X-Access-Token": token}, data={"template_id": "test"})
        self.assertTrue(res.status_code == 400)

    # Job API Test - POST Method, Create Job from Template - Template ID incorrect
    def test_POST_method_incorrect_template_id(self):
        res = self.app.post(f"/api/job/create", headers={"X-Access-Token": token}, data={"template_id": 2})
        self.assertTrue(res.status_code == 400)

    # Job API Test - POST Method, Create Job from Template - Template ID correct and job name not give
    def test_POST_method_correct_template_id_job_name_given(self):
        from GangaCore.GPI import JobTemplate, jobs

        # Create test template
        t = JobTemplate()
        t.name = "Test Template Name"

        # POST request
        res = self.app.post(f"/api/job/create", headers={"X-Access-Token": token},
                            data={"template_id": t.id, "job_name": "Custom Test Name"})
        self.assertTrue(res.status_code == 200)

        job_created = False
        for j in jobs:
            print(j.name)
            if j.name == "Custom Test Name":
                job_created = True

        # Assert if job was created
        self.assertTrue(job_created)

    # Job API Test - POST Method, Create Job from Template - Template ID correct and job name also provided
    def test_POST_method_correct_template_id_job_name_not_given(self):
        from GangaCore.GPI import JobTemplate, jobs

        # Create test template
        t = JobTemplate()
        t.name = "Test Template Name"

        # POST request
        res = self.app.post(f"/api/job/create", headers={"X-Access-Token": token}, data={"template_id": t.id})
        self.assertTrue(res.status_code == 200)

        job_created = False
        for j in jobs:
            print(j.name)
            if j.name == "Test Template Name":
                job_created = True

        # Assert job creation and job name
        self.assertTrue(job_created)

    # Job API Test - PUT Method, Action on Single Job - Job ID out of index
    def test_PUT_method_id_out_of_index(self):
        res = self.app.put(f"/api/job/1/name", headers={"X-Access-Token": token}, data={"name": "New Test Name"})
        self.assertTrue(res.status_code == 400)

    # Job API Test - PUT Method, Action on Single Job - Job ID negative
    def test_PUT_method_id_negative(self):
        res = self.app.put(f"/api/job/-1/id", headers={"X-Access-Token": token}, data={"name": "New Test Name"})
        self.assertTrue(res.status_code == 404)

    # Job API Test - PUT Method, Action on Single Job - Job ID string
    def test_PUT_method_id_string(self):
        res = self.app.put(f"/api/job/test/id", headers={"X-Access-Token": token}, data={"name": "New Test Name"})
        self.assertTrue(res.status_code == 404)

    # Job API Test - PUT Method, Action on Single Job - Job ID correct but unsupported action
    def test_PUT_method_unsupported_action(self):
        from GangaCore.GPI import Job

        # Create test job
        j = Job()
        j.name = "API Test Job"
        j.comment = "Test comment"

        # PUT request
        res = self.app.put(f"/api/job/{j.id}/test", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Job API Test - PUT Method, Action on Single Job - do_auto_resubmit action - no or bad form data
    def test_PUT_method_do_auto_resubmit_action_no_or_bad_value(self):
        from GangaCore.GPI import Job

        # Create test job and set do_auto_submit to false
        j = Job()
        j.name = "Do Auto Resubmit Test"
        j.do_auto_resubmit = False

        # PUT request without form data
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

        # PUT request with bad form data
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"do_auto_resubmit": '"Bad Value"'})
        self.assertTrue(res.status_code == 400)

    # Job API Test - PUT Method, Action on Single Job - do_auto_resubmit action - correct form data
    def test_PUT_method_do_auto_resubmit_action(self):
        from GangaCore.GPI import Job, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Create test job and set do_auto_submit to false
        j = Job()
        j.name = "Do Auto Resubmit Test"
        j.do_auto_resubmit = False
        j.comment = "Do Auto Resubmit Test Comment"
        j.application.exe = "sleep"
        j.application.args = ["60"]
        j.backend = Local()

        # PUT request with correct form data
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"do_auto_resubmit": 'true'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.do_auto_resubmit == True)

        # PUT request with correct form data
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"do_auto_resubmit": 'false'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.do_auto_resubmit == False)

        # PUT request with correct form data
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"do_auto_resubmit": 'true'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.do_auto_resubmit == True)

        # PUT request with correct form data
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"do_auto_resubmit": 'false'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.do_auto_resubmit == False)

        # Submit the job and wait for status to be running
        j.submit()
        sleep_until_state(j, state="running")
        self.assertTrue(j.status == "running")

        # Cannot change value in running state
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"do_auto_resubmit": 'true'})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.do_auto_resubmit == False)
        self.assertTrue(j.status == "running")

        # Kill the job
        j.kill()
        self.assertTrue(j.status == "killed")

        # Cannot change value in killed state
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"do_auto_resubmit": 'true'})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.do_auto_resubmit == False)
        self.assertTrue(j.status == "killed")

        # Change job status to failed
        j.force_status("failed")
        self.assertTrue(j.status == "failed")

        # Cannot change value in failed state
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"do_auto_resubmit": 'true'})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.do_auto_resubmit == False)
        self.assertTrue(j.status == "failed")

        # Change job status to completed
        j.force_status("completed")
        self.assertTrue(j.status == "completed")

        # Cannot change value in completed state
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"do_auto_resubmit": 'true'})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.do_auto_resubmit == False)
        self.assertTrue(j.status == "completed")

    # Job API Test - PUT Method, Action on Single Job - name action - no form data
    def test_PUT_method_name_action_no_value(self):
        from GangaCore.GPI import Job

        # Create test job
        j = Job()
        j.name = "Name 1"

        # PUT request without form data
        res = self.app.put(f"/api/job/{j.id}/name", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Job API Test - PUT Method, Action on Single Job - name action - correct form data
    def test_PUT_method_name_action(self):
        from GangaCore.GPI import Job, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Create test job
        j = Job()
        j.name = "Name 1"
        j.application.exe = "sleep"
        j.application.args = ["60"]
        j.backend = Local()

        # PUT request with correct form data
        res = self.app.put(f"/api/job/{j.id}/name", headers={"X-Access-Token": token}, data={"name": '"Name 2"'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.name == "Name 2")

        # Submit the job
        j.submit()
        sleep_until_state(j, state="running")
        self.assertTrue(j.status == "running")

        # Cannot change name in running state
        res = self.app.put(f"/api/job/{j.id}/name", headers={"X-Access-Token": token},
                           data={"name": '"Name 3"'})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.name == "Name 2")
        self.assertTrue(j.status == "running")

    # Job API Test - PUT Method, Action on Single Job - parallel_submit action - no or bad form data
    def test_PUT_parallel_submit_action_no_or_bad_value(self):
        from GangaCore.GPI import Job

        # Create test job
        j = Job()
        j.name = "Paralle Submit Test"
        j.parallel_submit = True

        # PUT request with no data
        res = self.app.put(f"/api/job/{j.id}/parallel_submit", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

        # PUT reqeust with bad data
        res = self.app.put(f"/api/job/{j.id}/do_auto_resubmit", headers={"X-Access-Token": token},
                           data={"parallel_submit": '"Bad Value"'})
        self.assertTrue(res.status_code == 400)

    # Job API Test - PUT Method, Action on Single Job - parallel_submit action - good form data
    def test_PUT_method_parallel_submit_action(self):
        from GangaCore.GPI import Job, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Create test job
        j = Job()
        j.name = "Parallel Submit Test"
        j.parallel_submit = True
        j.application.exe = "sleep"
        j.application.args = ["60"]
        j.backend = Local()

        self.assertTrue(j.status == "new")

        # PUT request with correct form data
        res = self.app.put(f"/api/job/{j.id}/parallel_submit", headers={"X-Access-Token": token},
                           data={"parallel_submit": "false"})
        print(res.json)
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.do_auto_resubmit == False)

        # PUT request with correct form data
        res = self.app.put(f"/api/job/{j.id}/parallel_submit", headers={"X-Access-Token": token},
                           data={"parallel_submit": 'true'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.parallel_submit == True)

        # PUT request with correct form data
        res = self.app.put(f"/api/job/{j.id}/parallel_submit", headers={"X-Access-Token": token},
                           data={"parallel_submit": 'false'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.parallel_submit == False)

        # PUT request with correct form data
        res = self.app.put(f"/api/job/{j.id}/parallel_submit", headers={"X-Access-Token": token},
                           data={"parallel_submit": 'true'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.parallel_submit == True)

        self.assertTrue(j.status == "new")

        # Submit the job
        j.submit()
        sleep_until_state(j, state="running")
        self.assertTrue(j.status == "running")

        # Cannot change value in running state
        res = self.app.put(f"/api/job/{j.id}/parallel_submit", headers={"X-Access-Token": token},
                           data={"parallel_submit": 'false'})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.parallel_submit == True)
        self.assertTrue(j.status == "running")

    # Job API - PUT Method, Kill action
    def test_PUT_kill_action(self):
        from GangaCore.GPI import Job, jobs, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Empty job repository
        self.assertTrue(len(jobs) == 0)

        # Test sleep job
        j = Job()
        j.name = "Kill Job Name"
        j.application.exe = "sleep"
        j.application.args = ["60"]
        j.backend = Local()

        self.assertTrue(len(jobs) == 1)

        # Cannot kill job in new state
        res = self.app.put(f"/api/job/{j.id}/kill", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.status == "new")

        # Submit the job
        j.submit()
        sleep_until_state(j, state="running")
        self.assertTrue(j.status == "running")

        # Kill the job in running state
        res = self.app.put(f"/api/job/{j.id}/kill", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "killed")

        # Cannot kill job in killed state
        res = self.app.put(f"/api/job/{j.id}/kill", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.status == "killed")

        # Change job state to failed
        j.force_status("failed")
        self.assertTrue(j.status == "failed")

        # Cannot kill job in failed state but it will return 200 OK HTTP code
        res = self.app.put(f"/api/job/{j.id}/kill", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "failed")

        # Change job state to completed
        j.force_status("completed")
        self.assertTrue(j.status == "completed")

        # Cannot kill job in completed state but it will return 200 OK HTTP code
        res = self.app.put(f"/api/job/{j.id}/kill", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "completed")

    # Job API - PUT Method, force_status action - no or bad value in request body
    def test_PUT_force_status_action_no_or_bad_value(self):
        from GangaCore.GPI import Job, jobs

        # Empty repository
        self.assertTrue(len(jobs) == 0)

        # Test job
        j = Job()
        j.name = "Force Status Test"

        self.assertTrue(j.status == "new")

        # Request without a body
        res = self.app.put(f"/api/job/{j.id}/force_status", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

        # Reuqest with a bad value in the body - Bad values are other than (completed, failed)
        res = self.app.put(f"/api/job/{j.id}/force_status", headers={"X-Access-Token": token},
                           data={"force_status": '"BadValue"'})
        self.assertTrue(res.status_code == 400)

    # Job API - PUT Method, force_status action - good request body
    def test_PUT_force_status_action(self):
        from GangaCore.GPI import Job, jobs, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Empty repository
        self.assertTrue(len(jobs) == 0)

        # Test job
        j = Job()
        j.name = "Force Status Test"
        j.application.exe = "sleep"
        j.application.args = ["60"]
        j.backend = Local()

        self.assertTrue(j.status == "new")

        # Cannot change job status from new to completed
        res = self.app.put(f"/api/job/{j.id}/force_status", headers={"X-Access-Token": token},
                           data={"force_status": '"completed"'})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.status == "new")

        # Submit the job
        j.submit()
        sleep_until_state(j, state="running")
        self.assertTrue(j.status == "running")

        # Change job status from running to failed
        res = self.app.put(f"/api/job/{j.id}/force_status", headers={"X-Access-Token": token},
                           data={"force_status": '"failed"'})
        print(res.json)
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "failed")

        # Change job status from failed to completed
        res = self.app.put(f"/api/job/{j.id}/force_status", headers={"X-Access-Token": token},
                           data={"force_status": '"completed"'})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "completed")

    # Job API - PUT Method, resubmit action
    def test_PUT_resubmit_action(self):
        from GangaCore.GPI import Job, jobs, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Empty repository
        self.assertTrue(len(jobs) == 0)

        # Test job
        j = Job()
        j.name = "Resubmit Test"
        j.application.exe = "sleep"
        j.application.args = ["5"]
        j.backend = Local()

        self.assertTrue(j.status == "new")

        # Cannot resubmit job with status new
        res = self.app.put(f"/api/job/{j.id}/resubmit", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.status == "new")

        # Submit the job
        j.submit()
        sleep_until_state(j)
        self.assertTrue(j.status == "completed")

        # Cannot resubmit job with status running
        res = self.app.put(f"/api/job/{j.id}/resubmit", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status != "completed")

    # Job API - PUT Method, submit action
    def test_PUT_submit_action(self):
        from GangaCore.GPI import Job, jobs, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Empty repository
        self.assertTrue(len(jobs) == 0)

        # Test job
        j = Job()
        j.name = "Submit Test"
        j.application.exe = "sleep"
        j.application.args = ["60"]
        j.backend = Local()

        self.assertTrue(j.status == "new")

        # Submit a job with status new
        res = self.app.put(f"/api/job/{j.id}/submit", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status != "new")

        sleep_until_state(j, state="running")
        self.assertTrue(j.status == "running")

        # Cannot submit job with status running
        res = self.app.put(f"/api/job/{j.id}/submit", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.status == "running")

        # Kill the job
        j.kill()
        self.assertTrue(j.status == "killed")

        # Cannot submit job with status killed
        res = self.app.put(f"/api/job/{j.id}/submit", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.status == "killed")

        # Force job status to failed
        j.force_status("failed")
        self.assertTrue(j.status == "failed")

        # Cannot submit job with status failed
        res = self.app.put(f"/api/job/{j.id}/submit", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.status == "failed")

        # Force job status to completed
        j.force_status("completed")
        self.assertTrue(j.status == "completed")

        # Cannot submit job with status failed
        res = self.app.put(f"/api/job/{j.id}/submit", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)
        self.assertTrue(j.status == "completed")

    # Job API - PUT Method, runPostProcessors action
    def test_PUT_runPostProcessors_action(self):
        from GangaCore.GPI import Job, jobs, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Empty repository
        self.assertTrue(len(jobs) == 0)

        # Test job
        j = Job()
        j.name = "runPostProcessors Test"
        j.application.exe = "sleep"
        j.application.args = ["60"]
        j.backend = Local()

        self.assertTrue(j.status == "new")

        # Run runPostProcessors of job with status new
        res = self.app.put(f"/api/job/{j.id}/runPostProcessors", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "new")

        # Submit the job
        j.submit()
        sleep_until_state(j, state="running")
        self.assertTrue(j.status == "running")

        # Run runPostProcessors of job with status running
        res = self.app.put(f"/api/job/{j.id}/runPostProcessors", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "running")

        # Kill the job
        j.kill()
        self.assertTrue(j.status == "killed")

        # Run runPostProcessors of job with status killed
        res = self.app.put(f"/api/job/{j.id}/runPostProcessors", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "killed")

        # Force job status to failed
        j.force_status("failed")
        self.assertTrue(j.status == "failed")

        # Run runPostProcessors of job with status failed
        res = self.app.put(f"/api/job/{j.id}/runPostProcessors", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "failed")

        # Force job status to completed
        j.force_status("completed")
        self.assertTrue(j.status == "completed")

        # Run runPostProcessors of job with status running
        res = self.app.put(f"/api/job/{j.id}/runPostProcessors", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(j.status == "completed")

    # Job API - DELETE Method, ID Out of Index
    def test_DELETE_method_id_out_of_range(self):
        res = self.app.delete(f"/api/job/1", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Job API - DELETE Method, ID is Negative
    def test_DELETE_method_id_negative(self):
        res = self.app.delete(f"/api/job/-1", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Job API - DELETE Method, ID is String
    def test_DELETE_method_id_string(self):
        res = self.app.delete(f"/api/job/test", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Job API - DELETE Method - Job in new state
    def test_DELETE_method_new_state(self):
        from GangaCore.GPI import Job, jobs, Local

        # Empty repository
        self.assertTrue(len(jobs) == 0)

        # Test job
        j = Job()
        j.name = "Delete Test"
        j.application.exe = "sleep"
        j.application.args = ["60"]
        j.backend = Local()

        self.assertTrue(j.status == "new")
        self.assertTrue(len(jobs) == 1)

        # Delete the job with status new
        res = self.app.delete(f"/api/job/{j.id}", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(jobs) == 0)
        self.assertTrue(j.id not in jobs.ids())

    # Job API - DELETE Method - Job in completed state
    def test_DELETE_method_completed_state(self):
        from GangaCore.GPI import Job, jobs, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Empty repository
        self.assertTrue(len(jobs) == 0)

        # Test job
        j = Job()
        j.name = "Delete Test"

        self.assertTrue(j.status == "new")
        self.assertTrue(len(jobs) == 1)

        j.submit()
        sleep_until_state(j)

        # Delete the job with status completed
        res = self.app.delete(f"/api/job/{j.id}", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(jobs) == 0)
        self.assertTrue(j.id not in jobs.ids())

    # Job API - DELETE Method - Job in failed state
    def test_DELETE_method_failed_state(self):
        from GangaCore.GPI import Job, jobs, Local
        from GangaTest.Framework.utils import sleep_until_state

        # Empty repository
        self.assertTrue(len(jobs) == 0)

        # Test job
        j = Job()
        j.name = "Delete Test"

        self.assertTrue(j.status == "new")
        self.assertTrue(len(jobs) == 1)

        j.submit()
        sleep_until_state(j)
        j.force_status("failed")

        # Delete the job with status completed
        res = self.app.delete(f"/api/job/{j.id}", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(jobs) == 0)
        self.assertTrue(j.id not in jobs.ids())

    # Tear down
    def tearDown(self):
        super(TestGangaGUIJobAPI, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))
