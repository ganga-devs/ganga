# import os
# import json
# from GangaCore.testlib.GangaUnitTest import GangaUnitTest

# class TestJobToFile(GangaUnitTest):


#     def test_job_to_file(self):
#         from GangaCore.GPI import Job
#         from GangaCore.GPIDev.Base import GangaObject
#         from GangaCore.GPIDev.Base.Proxy import stripProxy
#         from GangaCore.Core.GangaRepository.JStreamer import to_file

#         from tempfile import NamedTemporaryFile

#         j = Job()
#         stripped_j = stripProxy(j)
#         # Assert pre-submission job information
#         self.assertIn(j.status, ['new'])  
#         self.assertEqual(len(j.subjobs), 0)

#         temp_file = NamedTemporaryFile(mode = 'w', delete=False)
#         to_file(stripped_j, temp_file)
#         temp_file.flush()

#         self.assertTrue(os.path.exists(temp_file.name))

#         ## Check the attributes from the json
#         # Loading the json
#         fobj = open(temp_file.name, "r")
#         json_content = json.load(fobj)

#         # ForReview: 
#         # ["inputdir", "outputdir"] should be adapted to correspond to the gangadir testing repository
#         # ["fqid"] was added as that was causing errors and I do not know the use of this attribute
#         attr_blacklist = ["id", "inputdir", "outputdir", "fqid", "master"] # we do not check these for default values
#         # Check if the values are same as default values, the expected values
#         for attr in stripped_j._schema.allItemNames():
#             if attr not in attr_blacklist:
#                 attr_value = stripped_j._schema.getDefaultValue(attr)
#                 # checking for simple attributes
#                 if isinstance(attr_value, (bool, list, str)) or attr_value is None:
#                     print(attr_value, json_content[attr], attr_value == json_content[attr])
#                     self.assertEqual(attr_value, json_content[attr])
#                     # assert attr_value == json_content[attr], attr
#                     # self.assertEqual(attr_value, json_content[attr])
                
#                 # ForReview: Do we require the checking of attributes of components attributes like ["JobTime", "JobInfo", ...]
#                 # checking for component attributes
#                 else:
#                     print("Skipping the component object")

#         os.unlink(temp_file.name)



#     # This test assumes the file created by the above test still exists
#     # FIXME: Add a cleaner function to remove created files like: "test_job.job"
#     def test_job_from_file(self):
#         from GangaCore.GPI import Job
#         from GangaCore.GPIDev.Base import GangaObject
#         from GangaCore.Core.GangaRepository.JStreamer import from_file, to_file
#         from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy

#         from tempfile import NamedTemporaryFile

#         j = Job()
#         stripped_j = stripProxy(j)
#         temp_file = NamedTemporaryFile(mode = 'w', delete=False)
#         to_file(stripped_j, temp_file)
#         temp_file.flush()

#         # creating the job_json for testing the loading 

#         # Check if the created file by to_file exists
#         self.assertTrue(os.path.exists(temp_file.name))


#         fobj = open(temp_file.name, "r")
#         j, error = from_file(f=fobj)

#         proxy_j = addProxy(j)

#         # Assert if loaded job is the same as the job used to create the json
#         self.assertEqual(proxy_j, stripped_j)

#         # removing the file
#         os.unlink(temp_file.name)


#     # def test_job_to_file(self):
#     #     from GangaCore.GPI import Job
#     #     from GangaCore.GPIDev.Base import GangaObject
#     #     from GangaCore.Core.GangaRepository.JStreamer import to_file
#     #     from GangaCore.GPIDev.Base.Proxy import stripProxy



#     #     j = Job()

#     #     # Assert pre-submission job information
#     #     self.assertIn(j.status, ['new'])  
#     #     self.assertEqual(len(j.subjobs), 0)
        
#     #     stripped_j = stripProxy(j)
#     #     filename = "test_job.json"
#     #     fobj = open(filename, "w")
#     #     to_file(j=stripped_j, fobj=fobj)

#     #     # Check if the created file by to_file exists
#     #     self.assertTrue(os.path.exists(filename))

#     #     ## Check the attributes from the json
#     #     # Loading the json
#     #     fobj = open(filename, "r")
#     #     json_content = json.load(fobj)

#     #     # ForReview: 
#     #     # ["inputdir", "outputdir"] should be adapted to correspond to the gangadir testing repository
#     #     # ["fqid"] was added as that was causing errors and I do not know the use of this attribute
#     #     attr_blacklist = ["id", "inputdir", "outputdir", "fqid"] # we do not check these for default values
#     #     # Check if the values are same as default values, the expected values
#     #     for attr in stripped_j._schema.allItemNames():
#     #         if attr not in attr_blacklist:
#     #             attr_value = stripped_j._schema.getDefaultValue(attr)
#     #             # checking for simple attributes
#     #             if isinstance(attr_value, (bool, list, str)) or attr_value is None:
#     #                 self.assertEqual(attr_value, json_content[attr])
#     #                 # assert attr_value == json_content[attr], attr
#     #                 # self.assertEqual(attr_value, json_content[attr])
                
#     #             # ForReview: Do we require the checking of attributes of components attributes like ["JobTime", "JobInfo", ...]
#     #             # checking for component attributes
#     #             else:
#     #                 print("Skipping the component object")


#     #     # removing the file after use
#     #     os.remove(filename)

