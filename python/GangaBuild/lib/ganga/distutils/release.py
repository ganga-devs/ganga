"""
Contains the distutils release command of the dashboard build.

@license: Apache License 2.0
"""
"""
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
import commands
import os
import re
import shutil
import sys
import tempfile

from ConfigParser import ConfigParser
from datetime import datetime
from distutils.cmd import Command

os.environ['LANG'] = "en_US" # always work in English environment

class release(Command):
    """
    Implements the distutils release command for the dashboard.
    
    It manages component version and tagging in SVN accordingly (and
    branching for the cases of major/minor releases).

    @author: Ricardo Rocha <ricardo.rocha@cern.ch>
    @author: Sergey Belov <Sergey.Belov@cern.ch>
    @version: $Id: release.py,v 1.16 2010/03/30 15:54:11 sbelov Exp $
    """

    """
    Description of the module.
    """
    description = "release the module (updating version and tagging in svn)"

    """
    Listing of the command options (including description).
    """
    user_options = [("major", "M", "major version release"),
                    ("minor", "m", "minor version release"),
                    ("patch", "p", "patch release"),
                    ("candidate", "c", "candidate release"),
                    ("unstable", "u", "unstable release"),
                    ("stable", "s", "stable release"),
                    ("release-notes=", "r", "location of the RELEASE-NOTES file")]

    """
    List options that are boolean.
    """
    boolean_options = ["major", "minor", "patch", "candidate", "stable", "unstable"]

    """
    Listing of help related command options.
    """
    help_options = []

    def initialize_options(self):
        """
        Initialize command options.
        """
        self.major = False
        self.minor = False
        self.patch = False
        self.candidate = False
        self.stable = False
        self.unstable = False
        self.release_notes = "RELEASE-NOTES"
        self.build_base = None

    def finalize_options(self):
        """
        Finalize command options.
        """
        self.set_undefined_options('build', ('build_base', 'build_base'))

    def run(self):
        """
        Implementation of the release command.
        """
        # Make sure the build directory exists
        if not os.path.exists(self.build_base):
            os.mkdir(self.build_base)

        # Load the external module configuration
        self.config = ConfigParser()
        self.config.read(["module.cfg"])
        self.moduleName = str(self.config.get("module", "name"))

        # Parse the current version string
        currentVersion = self.config.get("module", "version")
        major, minor, patch = currentVersion.split(".")
        try:
            patch, candidate = patch.split("_rc")
        except ValueError:
            candidate = 0
        currentVersion = int(major), int(minor), int(patch), int(candidate)

        # Get module's URL in SVN
        (code, output) = commands.getstatusoutput("svn info")
        match = re.search("^URL: (.+)$", output, re.MULTILINE)
        if match is not None:
            self.module_repo_url = match.group(1)
        else:
            print >> sys.stderr, "Could not get module info from SVN. Possible the directory is not under SVN control?"
            sys.exit(1)

        # Get URL of SVN repository root
        match = re.search("^Repository Root: (.+)$", output, re.MULTILINE)
        if match is not None:
            self.repo_root_url = match.group(1)
        else:
            print >> sys.stderr, "Could not get repository info from SVN. Possible the directory is not under SVN control?"
            sys.exit(1)
        # Get last revision of the module
        match = re.search("^Last Changed Rev: (\d+)$", output, re.MULTILINE)
        if match is not None:
            self.module_last_revision = match.group(1)
        else:
            print >> sys.stderr, "Could not get last revision information: wrong svn output [%s]" % output
            sys.exit(1)

        # Extract module's relative path in SVN
        # (relative to /trunk, /branches/branch-name or /tags/tag-name)
        module_relative_path = self.module_repo_url.replace(self.repo_root_url, "")
        # The path of the module from the root of the repo, not from the 
        # trunk/branches/tags module dirs (see below)
        self.module_root_relative_path = module_relative_path
        mod_path_tmp = re.sub("^/(trunk|(branches/[^/]+/[^/]+))/", "", module_relative_path)
        if mod_path_tmp == module_relative_path:
            if re.match("^/tags/", module_relative_path):
                print >> sys.stderr, "You can't make a release from a tag. " + \
                                    "Please switch to the appropriate branch or to the trunk and apply your changes there. (And rollback any commits you may have made to this tag.)"
            else:
                print >> sys.stderr, "Wrong SVN repository or module information. Please update."
            sys.exit(1)
        self.module_relative_path = mod_path_tmp
        del mod_path_tmp
        del module_relative_path
        # Get module's name and relative directory
        match = re.match("^(.*/)*([^/]+)", self.module_relative_path)
        (mod_dir, mod_name) = match.groups()
        del match
        if (mod_dir is None) and (mod_name is None) :
            print >> sys.stderr, "Wrong module relative path: '%s' in '%s'" % (self.module_relative_path, self.module_repo_url)
            sys.exit(1)
        if (mod_dir is None):
            mod_dir = ""
        self.module_relative_dir = re.sub("/$", "", mod_dir) # remove trailing slash
        self.module_repo_name = mod_name
        del mod_dir
        del mod_name

        # Check that the module is up to date
        (code, output) = commands.getstatusoutput("svn status")

        if re.search("^[ADMR!~].*", output, re.M) is not None:
            print "\n-----------------\n", "Directory status:\n", output, "\n-----------------\n"
            noUpdateOk = raw_input("Module is not up to date. Are you sure you want " \
                                   "to release [Y/N]? ").upper()
            while noUpdateOk not in ["Y", "N"]:
                noUpdateOk = raw_input("Please provide either 'Y' or 'N': ").upper()
            if noUpdateOk == "N":
                print "Release cancelled. Version stays in %s" % self.getVersion(currentVersion)
                sys.exit(1)

        # Check if we are attempting to create a new release candidate without ever releasing
        # the previous version        
        if currentVersion[3] != 0 and (self.major or self.minor or self.patch):
            versionOk = raw_input("This will create a new version without ever actually releasing "\
                                  "the current one (%s is a candidate version).\nContinue [Y/N]: "
                                  % self.getVersion(currentVersion)).upper()
            while versionOk not in ["Y", "N"]:
                versionOk = raw_input("Please provide either 'Y' or 'N': ").upper()
            if versionOk == "N":
                print "Canceled released. Version stays in %s" % self.getVersion(currentVersion)
                sys.exit(1)

        # Check that we are realeasing a major/minor version from a trunk
        if (self.major or self.minor) and (re.match(".+/trunk(/.*|$)", self.module_repo_url) is None):
            print >> sys.stderr, "It seems you are working in branch. " \
                "Major/minor releases are only allowed from /trunk/"
            sys.exit(1)

        # Check that we are not realeasing a patch from /trunk/
        if (self.patch) and (re.match(".+/trunk(/.*|$)", self.module_repo_url) is not None):
            print >> sys.stderr, "It seems you are working in trunk. Patches should only be applied on branches."
            sys.exit(1)

        # Create the new version
        if self.major and not self.minor and not self.patch and not self.candidate:
            newVersion = currentVersion[0] + 1, 0, 0, 1
            self._candidateRelease(currentVersion, newVersion)
        elif self.minor and not self.major and not self.patch and not self.candidate:
            newVersion = currentVersion[0], currentVersion[1] + 1, 0, 1
            self._candidateRelease(currentVersion, newVersion)
        elif self.patch and not self.major and not self.minor and not self.candidate:
            newVersion = currentVersion[0], currentVersion[1], currentVersion[2] + 1, 1
            self._candidateRelease(currentVersion, newVersion)
        elif self.candidate and not self.major and not self.minor and not self.patch:
            if currentVersion[3] == 0:
                print >> sys.stderr, "You need to release a major/minor/patch first to have a new release candidate"
                sys.exit(1)
            newVersion = currentVersion[0], currentVersion[1], currentVersion[2], \
                         currentVersion[3] + 1
            self._candidateRelease(currentVersion, newVersion)
        else:
            if currentVersion[3] == 0:
                print >> sys.stderr, "Version %s has already been released. You need to release a candidate " \
                "major/minor/patch to create a new candidate." % self.getVersion(currentVersion)
                sys.exit(1)
            newVersion = currentVersion[0], currentVersion[1], currentVersion[2], 0
            self._release(currentVersion, newVersion)

        print "Released version %s" % self.config.get("module", "version")

    def _candidateRelease(self, currentVersion, newVersion):
        """
        Performs the steps required prior to a candidate release.
        
        Calls release(currentVersion, newVersion) afterwards.
        
        The reason for this is that it is not necessary when an actually release (not
        candidate is being performed).
        
        @param currentVersion: The current (already released) version of the module
        @param newVersion: The new version of the module (to be released)        
        """
        # Check if assumed version is ok
        releaseOk = raw_input("Current module version is %s. Released version will be %s.\n" \
                              "Continue with release? [Y/N]: "
                              % (self.getVersion(currentVersion),
                                 self.getVersion(newVersion))).upper()
        while releaseOk not in ["Y", "N"]:
            releaseOk = raw_input("Please provide either 'Y' or 'N': ").upper()
        if releaseOk == "N":
            print "Release cancelled. Version stays in %s" % self.getVersion(currentVersion)
            sys.exit(1)

        self._release(currentVersion, newVersion)

    def _release(self, currentVersion, newVersion):
        """
        Performs a release of the current module using the parameter newVersion.

        @param currentVersion: The current (already released) version of the module
        @param newVersion: The new version of the module (to be released)
        """

        (code, output) = commands.getstatusoutput("../arda.dashboard/bin/svn2cl.sh " \
                                                  "--group-by-day --reparagraph " \
                                                  "--file %s/ChangeLog " \
                                                  "-r HEAD:%s" % (self.build_base, self.module_last_revision))

        if code != 0:
            print >> sys.stderr, "Failed to generate change log\n%s" % output
            sys.exit(1)

        # Launch the editor for the RELEASE NOTES
        (code, output) = commands.getstatusoutput("xterm -e vim -f -o %s %s/ChangeLog " \
                                  "-c \":r !echo -e '*** Release Version: %s -- Date: %s\\n'\" -c 2"
                                  % (self.release_notes, self.build_base, self.getVersion(newVersion),
                                     datetime.now().strftime("%Y-%m-%d %H:%M")))
        if code != 0:
            print >> sys.stderr, "Failed to launch editor"
            sys.exit(1)

        should_make_branch = (newVersion[3] == 0 and newVersion[2] == 0)
        tag = self.getVersionTag(self.getVersion(newVersion))
        # Check again if the tag should be made
        tagOk = ""
        if should_make_branch:
            branch = tag + "-branch"
            tagOk = raw_input('Branch will be named "%s"; its corresponding tag will be "%s".\nContinue [Y/N]: ' % (branch, tag)).upper()
        else:
            tagOk = raw_input('Tag will be named "%s"\nContinue [Y/N]: ' % tag).upper()

        while tagOk not in ["Y", "N"]:
            tagOk = raw_input("Please provide either 'Y' or 'N': ").upper()
        if tagOk == "N":
            print "Canceled release/tag. Version stays in %s (but %s were updated)" \
                % (self.getVersion(currentVersion), self.release_notes)
            sys.exit(1)

        # Update the module version
        v = self.getVersion(newVersion)
        self.config.set("module", "version", v)
        if (self.stable):
            self.config.set("module_versions", "stable", v)
        if (self.unstable):
            self.config.set("module_versions", "unstable", v)
        self.config.write(open("module.cfg", "w"))

        # Commit changes on module.cfg and release notes files
        (code, output) = commands.getstatusoutput("svn commit -m 'release: %s' module.cfg %s"
                                                  % (self.getVersion(newVersion), self.release_notes))
        if code != 0:
            print >> sys.stderr, "Failed to commit module configuration and the release notes."
            print >> sys.stderr, output
            sys.exit(1)

        origin = self.module_root_relative_path
        if should_make_branch:
            branch_path = "branches/%s/%s" % (self.module_repo_name, branch)
            err = self._svncopyDir(self.repo_root_url, origin, branch_path, branch)
            if err != "":
                print >> sys.stderr, "Failed to create branch with error '%s'" % err
                sys.exit(1)
            origin = branch_path + "/" + self.module_repo_name

        tag_path = "tags/%s/%s" % (self.module_repo_name, tag)
        err = self._svncopyDir(self.repo_root_url, origin, tag_path, tag)
        if err != "":
            print >> sys.stderr, "Failed to create tag with error '%s'" % err
            sys.exit(1)

    def _svncopyDir(self, repo, dir_name, to, changelog):
        # Check if the destination directory already exists; if not - try to create it
        # with necessary nested directories.
        # This is a workaround for old svn clients which have no "--parents" option
        repo = re.sub("/*$", "", repo)
        _fp = lambda x: repo + "/" + x
        path = repo
        for dir in re.sub("/*$", "", to).split("/"):
            if dir != "":
                path += "/" + dir
            (code, output) = commands.getstatusoutput("svn list '%s'" % path)
            if code != 0:
                (code, output) = commands.getstatusoutput("svn mkdir %s -m'%s' " % (path, changelog))
                if code != 0:
                    return output
        (code, output) = commands.getstatusoutput("svn copy %s %s -m '%s'" % (_fp(dir_name), _fp(to), changelog))
        if code != 0:
            return 'Could not copy "%s" to "%s" (svn error: "%s")' % (_fp(dir_name), _fp(to), output)
        return ""




    def getVersion(self, version):
        """
        Returns a string representing the version (building it from each of the
        parameter tuple's elements.
        
        @param version: A tuple containing the version parts (major, minor, patch, candidate)
        
        @return: A string representing the version
        """
        if version[3] == 0:
            return "%d.%d.%d" % (version[0], version[1], version[2])
        else:
            return "%d.%d.%d_rc%d" % (version[0], version[1], version[2], version[3])

    def getVersionTag(self, version):
        """
        Builds and returns the tag corresponding to the given version of the current module.
        
        @param version: The version to build the tag from
        
        @return: A string representation of the tag corresponding to the given version of the current module
        """
        return "%s-%s" % (self.moduleName, version.replace(".", "-"))


