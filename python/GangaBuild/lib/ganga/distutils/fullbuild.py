"""
Contains the Ganga fullbuild command.

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
import os.path
import shutil
import smtplib
import socket
import sys
import types
import errno
import time
import urllib
import urllib2
import re

try: #This is python 2.6
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email.mime.text import MIMEText
    from email.utils import formatdate
    import email.encoders as Encoders
except:
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEBase import MIMEBase
    from email.MIMEText import MIMEText
    from email.Utils import formatdate
    from email import Encoders

from distutils.cmd import Command

from xml.dom import minidom
from ganga.distutils.xmlutils import xpath

os.environ['LANG'] = "en_US" # always work in English environment

class fullbuild(Command):
    """
    Implementation of the fullbuild command for the Ganga build.
    
    Takes a ganga-build xml config file and performs a complete build
    of the Ganga software. Optionally it can generate an APT repository
    out of the generated rpms.
    
    @author: Ricardo Rocha <ricardo.rocha@cern.ch>
    @version: $Id: fullbuild.py,v 1.43 2009/02/18 13:23:25 rocha Exp $
    """

    description = "performs a full Ganga build"

    user_options = [
        ('build-file=', 'f', 'Ganga build config file to use'),
        ('build-external-file=', 'f', 'Ganga externals build config file to use'
         + 'default \'config/build/ganga-externals.xml\''),
        ('workspace=', 'w', 'alternative directory to use as temporary '
         + 'location of files (default \'/tmp/ganga-build\')'),
        ('destination=', 'd', 'destination of the build '
         + '(docs, bins, srcs, ...)'),
        ('apt=', 'a', 'location of the APT repository'),
        ('contact=', 'c', 'email to send reports on build failures (optional)'),
        ('build-name=', None, 'name of the builder, included in the mails sent (optional)'),
        ('build-email=', None, 'email of the builder, sender in the mails (optional)'),
        ('build-svnroot=', None, 'svnroot of repository where the build module is located'),
        ('build-module=', None, 'name of the build module to fetch from the build svnroot'),
        ('build-xsd=', None, 'xml schema to validate the build config files against (optional)'),
        ('svnroot=', None, 'svnroot where all the modules being built are located'),
        ('svnprefix=', None, 'the directory in SVN under which all the modules being built are located'),
        ("skip-apt", None, "Don't generate the apt repository (default: generate)"),
        ("skip-yum", None, "Don't generate the yum repository (default: generate)"),
#        ('release=', None, 'name of the release branch being built')
    ]

    boolean_options = ["skip-apt", "skip-yum"]

    help_options = []

    build_ok = False

    def initialize_options(self):
        """
        Initializes the options for this distutils command.
        """
        self.build_file = None
        self.build_external_file = "config/build/ganga-externals.xml"
        self.destination = None
        self.workspace = None
        self.apt = None

        self.contact = None
        self.build_name = None
        self.build_email = None
        self.build_svnroot = None
        self.build_module = None
        self.build_xsd = "config/build/ganga-build-0.3.0.xsd"
        self.svnroot = None
        self.svnprefix = None
        self.release = None

        self.skip_apt = 0
        self.skip_yum = 0

        # TODO: This should be picked up from the bdist_rpm command
        self.dist_dir = "dist"
        # TODO: This should be picked up from the stage command        
        self.stage_dir = "stage"


    def finalize_options(self):
        """
        Finalizes the options for this distutils commands.
        """
        if self.build_file is None or self.build_external_file is None:
            print >> sys.stderr, \
                "Please specify both a Ganga build and an external build config file"
            self._feedback("main",
                           "Missing build or external build config file.\n\n" \
                           "Build File: %s\nExternal Build: %s" % (self.build_file,
                                                                   self.build_external_file))
            return

        # Parse the build config files
        try:
            self.buildConfig = minidom.parse(self.build_file)
        except Exception, exc:
            print >> sys.stderr, "Malformed Ganga build config file"
            buildFile = open(self.build_file)
            content = buildFile.read()
            buildFile.close()
            self._feedback("main", "Malformed Ganga build config file.\n\nResult: " % exc,
                           attaches = {self.build_file: content})
            return

        try:
            self.buildExternalConfig = minidom.parse(self.build_external_file)
        except Exception, exc:
            buildExternalFile = open(self.build_external_file)
            content = buildExternalFile.read()
            buildExternalFile.close()
            self._feedback("main", "Malformed Ganga external build config file.\n\n" \
                           "Result: " % exc, attaches = {self.build_external_file: content})
            return

        # Validate both the build and externals build files
        procst, output = commands.getstatusoutput("xmllint --schema %s %s"
                                                  % (self.build_xsd, self.build_file))
        if procst != 0:
            print >> sys.stderr, "Build file did not validate... nothing was built"
            self._feedback("main", "Provided build file is not valid.\n\nResult: %s" % output,
                           attaches = { self.build_file: self.buildConfig.toxml() })
            return

        # Load the build parameters
        if self.build_name is None:
            self.build_name = xpath.findnode(self.buildConfig,
                                            '/gangaBuild/config/build-name').childNodes[0].nodeValue
        if self.build_email is None:
            self.build_email = xpath.findnode(self.buildConfig,
                                              '/gangaBuild/config/build-email').childNodes[0].nodeValue
        if self.contact is None:
            self.contact = xpath.findnode(self.buildConfig,
                                          '/gangaBuild/config/build-contact').childNodes[0].nodeValue
        if self.build_svnroot is None:
            self.build_svnroot = xpath.findnode(self.buildConfig,
                                                '/gangaBuild/config/build-svnroot').childNodes[0].nodeValue
        if self.build_module is None:
            self.build_module = xpath.findnode(self.buildConfig,
                                               '/gangaBuild/config/build-module').childNodes[0].nodeValue
        if self.svnroot is None:
            self.svnroot = xpath.findnode(self.buildConfig,
                                          '/gangaBuild/config/svnroot').childNodes[0].nodeValue
        if self.svnprefix is None:
            try:
                self.svnprefix = xpath.findnode(self.buildConfig,
                                                '/gangaBuild/config/svnprefix').childNodes[0].nodeValue
            except Exception, exc:
                self.svnprefix = ""
        if self.release is None:
            self.release = xpath.findnode(self.buildConfig,
                                          '/gangaBuild/config/release').childNodes[0].nodeValue

        if self.workspace is None and self.build_name is not None and \
           self.release is not None:
            self.workspace = "/tmp/%s-%s" % (str(self.build_name), str(self.release))
        elif self.workspace is None:
            self.workspace = "/tmp/ganga-build"

        self.build_ok = True

    def _buildAllModules(self):
        #First, let's take the list of all the modules
        if not os.path.exists('tags'):
            procSt, output = commands.getstatusoutput("svn co --depth immediates  %s/tags" % self.svnroot)

        procSt, output = commands.getstatusoutput("svn up tags")
        #Now, let's see if any of them has been modified
        procSt, output2 = commands.getstatusoutput("svn up tags/* --set-depth immediates")
        done = {}
        for line in output.split('\n') + output2.split('\n'):
            print line
            if not re.match('(At)|(Updated to) revision ', line):
                if len(line.split('/')) != 3:
                    print "Skipping it... it is a global directory"
                    continue
                m = re.split('\s+', line)
                moduleName = m[1]
                if done.has_key(moduleName):
                     continue
                done[moduleName] = 1
                procSt, dirName = commands.getstatusoutput("svn up %s --set-depth infinity" % moduleName)
                realDirName = re.split('\s+', dirName.split('\n')[0])[1]
                if not re.match(moduleName, realDirName):
                    print "The module", moduleName, " does not seem correct... got " , dirName
                else:
                    self._buildModule("%s/%s" % (self.workspace, realDirName))
                commands.getstatusoutput("svn up %s --set-depth empty" % moduleName)
        print "All modules have been built "

    def run(self):
        """
        Performs a full Ganga build.
        """
        if not self.build_ok:
            return

        # Prepare the local workspace
        self.workspaceTmp = os.path.join(self.workspace, "tmp")

        if not os.path.exists(self.workspace):
        #    shutil.rmtree(self.workspace)
            os.makedirs(self.workspace)
        if os.path.exists(self.workspaceTmp):
            shutil.rmtree(self.workspaceTmp)
        os.makedirs(self.workspaceTmp)


        # Move into the local workspace
        os.chdir(self.workspace)

        if not os.path.exists(self.dist_dir):
            os.makedirs(self.dist_dir)


        # Get and build the 'build' module first
        os.spawnlp(os.P_WAIT, 'svn', 'svn', '-q', 'checkout',
           "%s/%s" % (self.build_svnroot, self.build_module))
        os.chdir(self.build_module)

        procSt, output = commands.getstatusoutput("python setup.py stage")
        if procSt != 0 and self.contact is not None:
            self._feedback("main", output)
        os.chdir(self.workspace)
        if self.verbose:
            print "Finished processing build module"

        # Build the modules
        self._buildAllModules()

        # Copy the build to the destination dir if provided
        self._copyDestination()

        # Create/Update the APT repository if it is the case (apt provided)
        if self.apt is not None:
            self._buildAPT(self.buildExternalConfig)

    def _copyDestination(self):
        if self.destination is None:
            return
        if not os.path.exists(self.destination):
            if self.verbose:
                print "Provided destination directory does not exist. Creating '%s'" \
                        % self.destination
            os.makedirs(self.destination)

        # Copy the documentation
        self.mkpath(os.path.join(self.stage_dir, "doc"))
        docDir = os.path.join(self.destination, "doc")
        self._copyTree(os.path.join(self.stage_dir, "doc"), docDir)

        # Copy the bin and src tarballs
        binDir = os.path.join(self.destination, "bin")
        srcDir = os.path.join(self.destination, "src")
        self._copyTree(os.path.join(self.dist_dir, "bin"), binDir)
        self._copyTree(os.path.join(self.dist_dir, "src"), srcDir)
        # Copy the python libraries to the global lib directory (for api doc generation)
        libDir = os.path.join(self.destination, "lib")
        self._copyTree(os.path.join(self.workspace, self.stage_dir, "lib"), libDir)
        # Generate the API documentation
        apiDocDir = os.path.join(self.destination, "doc", "api")
        if os.path.exists(apiDocDir):
             shutil.rmtree(apiDocDir)
        os.system("epydoc --html -o %s %s/*" % (apiDocDir, libDir))

    def _buildModule(self, directory): #  moduleName, tag = None, version = None):
        """
        Builds a single module.
        
        It is assumed that any dependencies have been previously handled.
        
        @param moduleName: The name of the module to be processed
        """
        if self.verbose:
            print "  Processing module '%s' at  %s ..." % (directory, time.strftime("%d/%m/%Y %H:%M:%S UTC ", time.gmtime()))

        # Exit code is success by default
        exitc = 0
        moduleOutputs = {}

        os.chdir(directory)
        # Run the python distutils targets
        try:
            os.symlink('%s/arda.dashboard' % self.workspace, '../arda.dashboard')
        except:
            pass

#            build_lib_path = os.path.join(self.workspace, self.stage_dir, "lib")
#            if os.environ.has_key("PYTHONPATH"):
#                os.environ['PYTHONPATH'] = build_lib_path + os.pathsep + os.environ['PYTHONPATH']
#            else:
#                os.environ['PYTHONPATH'] = build_lib_path
#            del build_lib_path
        allCom = [['stage', '-s %s/stage' % self.workspace],
                  ['sdist', '-d %s/dist_tar/' % self.workspace],
                  ['bdist', '-d %s/dist_tar/' % self.workspace],
                  ['bdist_rpm', '-d %s/dist/' % self.workspace]]

        errors = ''
        for (c, options) in allCom:

            procSt, moduleOutputs[c] = commands.getstatusoutput("python setup.py %s %s" % (c, options))
            if procSt != 0:
                errors += c + ' '
                print "    The %s failed" % (c)

        # TODO: Also run the tests

        # Check for errors during build and email results when bad
        if errors != ''  and self.contact is not None:
            self._feedback(directory, "Failed to build module '%s'. Check %s " % (directory, errors),
                           attaches = moduleOutputs)

        # Go back to the workspace dir
        os.chdir(self.workspace)
        if self.verbose:
            print "  Finished processing module '%s'..." % directory

    def _buildAPT(self, buildExternalConfig):
        """
        Builds the APT repository.
        
        Copies the RPMS and SRPMS for the requested release to the APT repository
        location and runs the APT metadata generation command on it (genbasedir).
        
        Also the external components are download as necessary.
        
        @param buildExternalConfig: The XML document containing the external
        dependencies build config file
        """
        print "Building the apt repository"

        externalsDir = os.path.join(self.apt, "RPMS.external")
        externalsTmpDir = os.path.join(self.workspace, "tmp")
        changes=0 

        for d in (self.apt, externalsDir, externalsTmpDir):
            if not os.path.exists(d):
                if self.verbose:
                    print "  directory", d, "  did not exist in destination. Creating..."
                os.makedirs(d)

        # Copy the generated RPMS/SRPMS to the APT repository
        if self.release != 'automatic':
            allDirs = ("SRPMS.%s" % self.release, "RPMS.%s" % self.release)
        else:
            allDirs = ('SRPMS.unstable', 'RPMS.unstable', 'SRPMS.stable', 'RPMS.stable')
        for dir in allDirs:
            m = os.path.join(self.apt, dir)
            if not os.path.exists(m):
                print "  SRPMS directory (" + m + ") did not exist in APT. Creating..."
                os.makedirs(m)

        for rpmFile in os.listdir(self.dist_dir):
            target = None

            if rpmFile.endswith('src.rpm'):
                target = 'SRPMS.'
            elif rpmFile.endswith('rpm'):
                target = 'RPMS.'
            if target:
                print "    We have to copy ", rpmFile
                realRelease = self.release
                if realRelease == 'automatic':
                    if (re.search('_rc\d', rpmFile)):
                        realRelease = 'unstable'
                    else:
                        realRelease = 'stable'
                changes = 1 
                shutil.copyfile(os.path.join(self.dist_dir, rpmFile),
                                os.path.join(self.apt, target + realRelease, rpmFile))
                if self.release == 'automatic':
                    print "      And deleting the source "
                    os.remove(os.path.join(self.dist_dir, rpmFile))
        print "    Doing the external repository"
        # Process the external components and copy the RPMS to APT

        # Process each component in the externals build file
        components = xpath.find(buildExternalConfig, '/gangaBuild/externals/component')
        for component in components:
            name = component.hasAttribute("name")
            if self.verbose:
                print "      Processing external component '%s'..." % name
            releases = xpath.find(component, "./release")

            # Process each release for the current component
            for release in releases:
                uri = release.getAttribute("uri")
                version = release.getAttribute("version")

                srcRpmName = uri.split('/')[-1]
                rpmName = srcRpmName.replace('.src', '.i386')

                # If the RPM is already in the APT repository, 
                # jump to the next
                if os.path.exists(os.path.join(externalsDir, rpmName)):
                    continue

                # If not and the link points to an binary RPM, just put it in 
                # the externals APT dir
                if not release.getAttribute("type"):
                    os.chdir(externalsDir)
                    try:
                        uriType = release.getAttribute("uriType")
                        if uriType == "local":
                            shutil.copy(uri, os.getcwd())
                        else:
                            urllib2.urlopen(uri)
                            fileName = uri.split('/')[-1]
                            urllib.urlretrieve(uri, fileName)
                        changes = 1 
                    except Exception, exc:
                        print >> sys.stderr, "Failed to download package. Location: %s" % uri
                        self._feedback("externals", "Failed to download package.\n\n" \
                                       "Location: %s\n\nOutput: %s" % (uri, exc))

                # If not but it is a src RPM, put it in the externals tmp dir,
                # generate the RPM and finally put it in the externals APT dir
                if release.hasAttribute("type"):
                    os.chdir(externalsTmpDir)
                    try:
                        uriType = release.attributes["uriType"]
                        try:
                            shutil.copy(uri, os.getcwd())
                        except IOError, exc:
                            print >> sys.stderr, "Could not access file '%s'. Output: %s" \
                                % (uri, exc)
                            self._feedback("externals", "Could not access file '%s'. Output: %s" \
                                           % (uri, exc))

                    except KeyError:
                        (code, output) = commands.getstatusoutput("wget %s" % uri)
                        if code != 0:
                            print >> sys.stderr, "Failed to download package. Location: %s" % uri
                            self._feedback("externals", "Failed to download package.\n\n" \
                                           "Location: %s\n\nOutput: " % (uri, output))

                    # There might be some environment variables to be set first
                    envVariables = xpath.find(release, "./env-var")

                    for var in envVariables:
                        os.environ[var.getAttribute("name")] = var.getAttribute("value")

                    # And now proceed to build
                    if self.verbose:
                        print "Generating RPM from '%s'..." % (srcRpmName)
                    (code, output) = commands.getstatusoutput("rpmbuild --rebuild %s" % (srcRpmName))
                    if code == 0:
                        shutil.copy(os.path.join("/usr/src/redhat/RPMS/i386", rpmName), externalsDir)
                    else:
                        print >> sys.stderr, "Failed to build rpm from source :: %s" % (srcRpmName)
                        self._feedback("externals", "Failed to build rpm from source. " \
                                       "\n\nName: %s\n\nURI: %s\n\nOutput: %s"
                                       % (srcRpmName, uri, output))

        # Finally we can generate the APT repository metadata
        if not self.skip_apt:
            if self.verbose:
                print "Creating the APT repository metadata..."
            procst, output = commands.getstatusoutput("genbasedir --progress --flat --bz2only %s"
                                                      % self.apt)
            if procst != 0:
                print >> sys.stderr, "Failed to create APT repository. Output: %s" % (output)
                self._feedback("apt", "Failed to create APT repository. Output: %s" % (output))

        # And create also the YUM repository from the same set of RPMs
        if not self.skip_yum:
            if changes == 0:
                print " No need to rebuild the yum repo... There were no changes"
            else:
                if self.verbose:
                    print "Creating the YUM repository metadata..."
                dirs = [self.release]
                if self.release == 'automatic':
                    dirs = ['unstable', 'stable']
                dirs.append('external')
                for d in dirs:
                    procst, output = commands.getstatusoutput("createrepo %s/RPMS.%s" % (self.apt, d))
                    if procst != 0:
                        print >> sys.stderr, "Failed to create YUM repository. Output: %s" % (output)
                        self._feedback("apt", "Failed to create YUM repository. Output: %s" % (output))

    def _feedback(self, module, text, attaches = {}):
        """
        Sends feeback back to the build contact provided (via email).
        
        @param module: The name of the module the feedbacks corresponds to
        @param text: The message body
        @param attaches: A list of all attachments to include in the message
        """
        sender = "%s <%s" % (self.build_name, self.build_email)
        subject = "Build '%s' [%s]: '%s' failed" % (self.build_name, socket.gethostname(),
                                                    module)

        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = self.contact
        msg['Date'] = formatdate(localtime = True)
        msg['Subject'] = subject

        # Main text
        msg.attach(MIMEText(text))

        for attach in attaches:
            part = MIMEBase("text", "plain")
            part.set_payload(attaches[attach])
            Encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment; filename="%s"' % attach)
            msg.attach(part)

        # Send the email
        smtp = smtplib.SMTP('localhost')
        smtp.sendmail(sender, self.contact, msg.as_string())
        smtp.close()

    def _copyTree(self, src, dst):
        """
        Copies the entire contents of directory 'src' inside 'dst', overwriting files if
        they already exist (which is why the shutil copy_tree method would not work.
        
        @param src: The directory where the files to be copied reside
        @param dst: The destination directory where all files should be put
        """
        names = os.listdir(src)
        if not os.path.exists(dst):
            os.mkdir(dst)
        errors = []
        for name in names:
            srcname = os.path.join(src, name)
            dstname = os.path.join(dst, name)
            try:
                if os.path.isdir(srcname):
                    self._copyTree(srcname, dstname)
                else:
                    shutil.copy2(srcname, dstname)
            except (IOError, os.error), why:
                errors.append((srcname, dstname, why))
        if errors:
            raise Exception, errors
    def __del__(self):
        if getattr(self, 'buildExternalConfig', None) is not None:
            self.buildExternalConfig.freeDoc()

