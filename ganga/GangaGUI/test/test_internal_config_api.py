from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.api import internal


# ******************** Test Class ******************** #


# Config API Tests
class TestGangaGUIInternalConfigAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUIInternalConfigAPI, self).setUp(extra_opts=[])

        # App config and database creation
        internal.config["TESTING"] = True

        # Flask test client
        self.app = internal.test_client()

    # Config API - GET Method
    def test_GET_method_config_list(self):
        from GangaCore.GPI import config
        from GangaCore.Utility.Config import getConfig

        # To store sections of config
        list_of_sections = []

        # Get each section information and append to the list
        for section in config:

            config_section = getConfig(section)
            options_list = []

            # Get options information for the particular config section
            for o in config_section.options.keys():
                options_list.append({
                    "name": str(config_section.options[o].name),
                    "value": str(config_section.options[o].value),
                    "docstring": str(config_section.options[o].docstring),
                })

            # Append config section data to the list
            list_of_sections.append({
                "name": str(config_section.name),
                "docstring": str(config_section.docstring),
                "options": options_list,
            })

        res = self.app.get(f"/internal/config")
        # print(res.json) # to check the res body
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(res.json) == len(list_of_sections))

    # Tear down
    def tearDown(self):
        super(TestGangaGUIInternalConfigAPI, self).tearDown()

# ******************** EOF ******************** #
