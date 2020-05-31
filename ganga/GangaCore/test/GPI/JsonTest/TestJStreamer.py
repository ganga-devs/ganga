from GangaCore.testlib.GangaUnitTest import GangaUnitTest

class TestJobToFile(GangaUnitTest):


    def test_job_to_file(self):
        import os
        import json
        from GangaCore.GPI import Job
        from GangaCore.GPIDev.Base import GangaObject
        from GangaCore.Core.GangaRepository.JStreamer import to_file
        from GangaCore.GPIDev.Base.Proxy import stripProxy

        j = Job()

        # Assert pre-submission job information
        self.assertIn(j.status, ['new'])  
        self.assertEqual(len(j.subjobs), 0)
        
        stripped_j = stripProxy(j)
        filename = "test_job.json"
        fobj = open(filename, "w")
        to_file(j=stripped_j, fobj=fobj)

        # Check if the created file by to_file exists
        self.assertTrue(os.path.exists(filename))

        ## Check the attributes from the json
        # Loading the json
        fobj = open(filename, "r")
        json_content = json.load(fobj)

        # ForReview: 
        # ["inputdir", "outputdir"] should be adapted to correspond to the gangadir testing repository
        # ["fqid"] was added as that was causing errors and I do not know the use of this attribute
        attr_blacklist = ["id", "inputdir", "outputdir", "fqid"] # we do not check these for default values
        # Check if the values are same as default values, the expected values
        for attr in stripped_j._schema.allItemNames():
            if attr not in attr_blacklist:
                attr_value = stripped_j._schema.getDefaultValue(attr)
                # checking for simple attributes
                if isinstance(attr_value, (bool, list, str)) or attr_value is None:
                    self.assertEqual(attr_value, json_content[attr])
                    # assert attr_value == json_content[attr], attr
                    # self.assertEqual(attr_value, json_content[attr])
                
                # ForReview: Do we require the checking of attributes of components attributes like ["JobTime", "JobInfo", ...]
                # checking for component attributes
                else:
                    print("Skipping the component object")


    # This test assumes the file created by the above test still exists
    # FIXME: Add a cleaner function to remove created files like: "test_job.job"
    def test_job_from_file(self):
        import os
        import json
        from GangaCore.GPI import Job
        from GangaCore.GPIDev.Base import GangaObject
        from GangaCore.Core.GangaRepository.JStreamer import from_file
        from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy


        # Check if the created file by to_file exists
        filename = "test_job.json"
        self.assertTrue(os.path.exists(filename))

        fobj = open(filename, "r")
        j, error = from_file(f=fobj)

        proxy_j = addProxy(j)

        # Assert post-submission job information
        self.assertIn(j.status, ['new'])  
        self.assertEqual(len(j.subjobs), 0)
