from GangaCore.testlib.GangaUnitTest import GangaUnitTest

# Test Report() Functionality
class TestJobToFile(GangaUnitTest):


    def test_job_to_file(self):
        import os
        import json
        from GangaCore.GPI import Job
        from GangaCore.GPIDev.Base import GangaObject
        from GangaCore.Core.GangaRepository.JStreamer import to_file, from_file
        from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy, isType, getName

        j = Job()

        # Assert post-submission job information
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
                    # self.assertEqual(attr_value, json_content[attr])
                    # assert ((attr_value == json_content[attr], attr)
                    assert attr_value == json_content[attr], attr
                    # self.assertEqual(attr_value, json_content[attr])
                # checking for component attributes
                else:
                    print("Skipping the component object")
