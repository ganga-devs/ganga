"""
Contains the distutils doc command extension for the Ganga build.

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
import sys

from distutils.cmd import Command

class doc(Command):
    """
    Implements the distutils doc extension command for the Ganga build.
    
    @author: Ricardo Rocha <ricardo.rocha@cern.ch>
    @version: $Id: doc.py,v 1.24 2008/03/26 16:22:40 bgaidioz Exp $
    """

    description = "generate the documentation (source code and more)"

    user_options = []

    boolean_options = []

    help_options = []
    
    def initialize_options(self):
        self.doc_base = None
        self.bdoc_base = None
        self.build_base = None

    def finalize_options(self):
        self.doc_base = "doc"
        if self.bdoc_base is None:
            self.bdoc_base = os.path.join("build", self.doc_base)
        self.bdoc_guides = os.path.join(self.bdoc_base, "guides")
        self.bdoc_man = os.path.join(self.bdoc_base, "man")        

    def run(self):
        self.set_undefined_options('build', ('build_base', 'build_base'))
        if os.path.exists(self.bdoc_base):
            if not os.path.exists("%s.building" % self.bdoc_base):
               return
            self.warn("Someone was building it...")

        open("%s.building" % self.bdoc_base, 'w').close()         
        print "File %s.building created" % self.bdoc_base
        self.mkpath(self.bdoc_base)

        docbookUtils = os.path.join(self.bdoc_base, "docbook-utils")
        if os.path.exists("doc"):
            if os.path.exists("doc/docbook-utils"):
                if os.path.exists(docbookUtils):
                    os.remove(docbookUtils)
                os.symlink("../../doc/docbook-utils", docbookUtils)
            else:
                if os.path.exists(docbookUtils):
                    os.remove(docbookUtils)                
                os.symlink("../../../arda.dashboard/config/doc/docbook-utils", docbookUtils)

        guidesDir = os.path.join("doc", "guides")
        # Build the guides one by one
        if os.path.exists(guidesDir):
            guides = os.listdir(guidesDir)
            for guide in guides:
                if guide == '.svn':
                    continue
                
                guideSource = "%s/guides/%s/%s-guide.xml" % (self.doc_base, guide, guide)
                
                guideOutputs = {}
                # Check the validity of the docbook code
                procSt, guideOutputs["validation"] = commands.getstatusoutput(
                                  "xmllint --noout --valid %s" % guideSource)
                
                # Generate the several guide formats
                
                #if not os.path.exists(os.path.join(self.bdoc_guides, "txt")):
                #    self.mkpath(os.path.join(self.bdoc_guides, "txt"))
                #    procSt, guideOutputs["txt"] = commands.getstatusoutput(
                #                   "jw -f docbook -b txt " \
                #                   "--output %s/txt %s" \
                #                   % (self.bdoc_guides, guideSource))
                #
                #if not os.path.exists(os.path.join(self.bdoc_guides, "pdf")):
                #    self.mkpath(os.path.join(self.bdoc_guides, "pdf"))
                #    procSt, guideOutputs["pdf"] = commands.getstatusoutput(
                #                   "jw -f docbook -b pdf " \
                #                   "--output %s/pdf %s" \
                #                   % (self.bdoc_guides, guideSource))
                    
                if not os.path.exists(os.path.join(self.bdoc_guides, 
                                                   "html", guide)):
                    self.mkpath(os.path.join(self.bdoc_guides, "html", guide))
                    procSt, guideOutputs["html"] = commands.getstatusoutput(
                                        "xsltproc --output %s/html/%s/index.html " \
                                        "%s/custom-xsl/xhtml-chunked.xsl " \
                                        "%s" 
                                        % (self.bdoc_guides, guide, docbookUtils,
                                           guideSource))
                if procSt:
                    self.warn("Error doing xsltproc: %s" % guideOutputs["html"]);
                    os.rmdir(os.path.join(self.bdoc_guides,"html", guide))
                    sys.exit(1);

                # Send out the outputs from the processes
                for output in guideOutputs.values():
                    print output
                    
        # Generate the man pages
        manPagesDir = os.path.join(self.doc_base, "man")
        bmanDir = os.path.join(self.bdoc_base, "man")
        if not os.path.exists(bmanDir):
            os.mkdir(bmanDir)
        if os.path.exists(manPagesDir) and len(os.listdir(manPagesDir)) != 0:
            if not os.path.exists(os.path.join(bmanDir, "common")):
                if os.path.exists("doc/man/common"):
                    os.symlink("../../../doc/man/common", os.path.join(bmanDir, "common"))
                else:
                    os.symlink("../../../../arda.dashboard/config/doc/man/common", 
                               os.path.join(bmanDir, "common"))
                        
            manPages = os.listdir(manPagesDir)
            manRegexp = re.compile(".*1\.xml")
            for manPage in manPages:
                if manRegexp.match(manPage):
                    toolName = manPage.split('.1.xml')[0]
                    (statusMan, outMan) = commands.getstatusoutput("xsltproc -o %s/man1/ " \
                                                                "%s/xsl/manpages/docbook.xsl %s/man/%s" \
                                                                % (self.bdoc_man, docbookUtils, self.doc_base, manPage))
                    (statusHtml, outHtml) = commands.getstatusoutput("xsltproc -o %s/html/%s.html " \
                                                                 "%s/xsl/xhtml/docbook.xsl %s/man/%s" \
                                                                 % (self.bdoc_man, toolName, docbookUtils, self.doc_base, manPage))
                    if statusMan != 0 or statusHtml != 0:
                        self.warn("WARNING: Failed to generate man page: %s. MAN: %s HTML: %s\n" 
                                  % (manPage, outMan, outHtml))
        os.remove("%s.building" % self.bdoc_base);

