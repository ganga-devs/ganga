##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: MigrationControl.py,v 1.1 2008-07-17 16:40:56 moscicki Exp $
##########################################################################

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

import Ganga.Utility.Config
config = Ganga.Utility.Config.makeConfig(
    'MigrationControl', 'migration of different job versions in the peristent repository')
##config.addOption('migration','interactive','selecting plugins to be migrated interactively')
##config.addOption('migration','allow','select all possible plugins for the migration')
config.addOption('migration', 'deny', 'no plugin migration')
config.addOption('display', 'compact',
                 'display limited number of choices for the interactive migration')
## config.addOption('display','full','display complete number of choices for the interactive migration')

##########################################################################
# class controlling migration process in case of incompatible schemas
# this module has to have an instance of this class called "migration"


class MigrationControl(object):

    _choices_compact = {
        '1': 'Yes',
        '2': 'No',
        '3': 'Yes for All',
        '4': 'No for All'}

    _choices_full = {
        '1': 'Yes',
        '2': 'No',
        '3': 'Yes for All',
        '4': 'No for All',
        '5': 'Yes for All with this version',
        '6': 'No for All with this version',
        '7': 'Yes for All with this name',
        '8': 'No for All with this name',
        '9': 'Yes for All within this category',
        '10': 'No for All within this category'}

    class TreeNode(type({})):
        # helper class

        def __init__(self, subtree, flag=None):
            self.flag = flag
            super(MigrationControl.TreeNode, self).__init__(subtree)

        def is_allowed(self):
            return self.flag

        def allow(self):
            self.flag = True
            # set allow flag in all children
            for k in self:
                self[k].allow()

        def deny(self):
            self.flag = False
            # set deny flag in all children
            for k in self:
                self[k].deny()

        def get(self, key, def_value=None):
            if not def_value:
                def_value = MigrationControl.TreeNode({})
            if key not in self:
                self[key] = def_value
                self[key].flag = self.flag  # child inherits flag
            return self[key]

    def __init__(self, display='full'):
        # dictionary where user answers are remembered
        # by default silent migration for all possible plugins is not allowed
        self._all_categs = self.TreeNode({})
        self.display = display

    def isAllowed(self, category, name, version, msg=''):
        """This method checks whether migration for the plugin called 'name' of version 'version'
        from category 'category' is allowed or not. If migration is not explicitly allowed through
        MigrationControl.migration object it will warn user with the message 'msg' and
        ask for permission to allow migration.
        """
        res = self._all_categs.get(category).get(
            name).get(version).is_allowed()
        if res == None:
            # ask for the user input
            answer = self.getUserInput(msg)
            if answer == 'Yes':
                res = True
            elif answer == 'No':
                res = False
            elif answer == 'Yes for All with this version':
                self.allow(category=category, name=name, version=version)
                res = True
            elif answer == 'No for All with this version':
                self.deny(category=category, name=name, version=version)
                res = False
            elif answer == 'Yes for All with this name':
                self.allow(category=category, name=name)
                res = True
            elif answer == 'No for All with this name':
                self.deny(category=category, name=name)
                res = False
            elif answer == 'Yes for All within this category':
                self.allow(category=category)
                res = True
            elif answer == 'No for All within this category':
                self.deny(category=category)
                res = False
            elif answer == 'Yes for All':
                self.allow()
                res = True
            elif answer == 'No for All':
                self.deny()
                res = False
            else:
                # any other answer
                res = True
        return res

    def allow(self, category=None, name=None, version=None):
        """This method allows migration.
        allow() --> allow all
        allow(category)--> allow all from 'category'
        allow(category, name) --> allow for all versions of 'name' from 'category'
        allow(category, name, version) --> allow for 'version' and 'name' from 'category'
        """
        # allow flag takes precedence of deny flag
        if category == None:
            # allow all possible migration
            self._all_categs.allow()
        elif name == None:
            # allow all in the category
            self._all_categs.get(category).allow()
        elif version == None:
            # allow all versions
            self._all_categs.get(category).get(name).allow()
        else:
            # allow particular version
            self._all_categs.get(category).get(name).get(version).allow()

    def deny(self, category=None, name=None, version=None):
        """This method denies migration.
        deny() --> deny all
        deny(category)--> deny all from 'category'
        deny(category, name) --> deny for all versions of 'name' from 'category'
        deny(category, name, version) --> deny for 'version' and 'name' from 'category'
        """

        # deny is opposite to allow
        if category == None:
            # deny all possible migration
            self._all_categs.deny()
        elif name == None:
            # deny all in the category
            self._all_categs.get(category).deny()
        elif version == None:
            # deny all versions
            self._all_categs.get(category).get(name).deny()
        else:
            # deny particular version
            self._all_categs.get(category).get(name).get(version).deny()

    def getUserInput(self, msg):
        """This method is for getting user permission for migration.
        Args: msg - is the warning message.
        """
        logger.warning(msg)
        prompt = "Would you like to migrate the plugin(s)?\n"
        prompt += "Once migrated they will be not backward compatible\n"
        prompt += "Please make your choice:\n"
        if self.display == 'compact':
            choices = self._choices_compact
        else:
            choices = self._choices_full
        chcs = sorted(map(int, choices.keys()))
        chcs = map(str, chcs)
        for k in chcs:
            prompt += "%s : %s\n" % (k, choices[k])
        prompt += "Any other key == 'Yes'\n"
        aid = raw_input(prompt)
        return choices.get(aid, '')


migration = MigrationControl()
migration.display = config['display']
if config['migration'] == 'allow':
    migration.allow()
elif config['migration'] == 'deny':
    migration.deny()

# list of migrated jobs
migrated_jobs = []
