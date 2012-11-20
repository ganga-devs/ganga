"""
Contains the distutils setup method extension of the Ganga build.

@license: Apache License 2.0
"""
"""
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
import distutils.core

from ConfigParser import ConfigParser

from ganga.distutils.build import build
from ganga.distutils.clean import clean
from ganga.distutils.doc import doc
from ganga.distutils.fullbuild import fullbuild
from ganga.distutils.gwt import gwt
from ganga.distutils.release import release
from ganga.distutils.sdist import sdist
from ganga.distutils.bdist_dumb import bdist_dumb
from ganga.distutils.bdist_rpm import bdist_rpm
from ganga.distutils.stage import stage

def setup(cmdclass = None, name = None, version = None, description = None, long_description = None, author = None,
          author_email = None, url = None, packages = None, package_dir = None, data_files = None,
          license = None, ext_modules = None):

    configItems = {"cmdclass": cmdclass, "name": name, "version": version,
                   "description": description, "long_description": long_description,
                   "author": author, "author_email": author_email, "license": license, "url": url,
                   "packages": packages, "package_dir": package_dir, "data_files": data_files,
                   "ext_modules" : ext_modules}

    # Make available all ganga command extension (default)
    configItems["cmdclass"] = {"build": build, "clean": clean, "doc": doc,
                               "fullbuild": fullbuild, "gwt": gwt,
                               "release": release,
                               "sdist": sdist, "bdist_dumb": bdist_dumb,
                               "bdist_rpm": bdist_rpm, "stage": stage
                               }

    # Load the external module configuration
    config = ConfigParser()
    configFile = open('module.cfg')
    config.readfp(configFile)
    configFile.close()
    for item in config.items("module"):
        if configItems[item[0]] is None:
            configItems[item[0]] = item[1]

    distutils.core.setup(cmdclass = configItems["cmdclass"], name = configItems["name"],
                         version = configItems["version"], description = configItems["description"],
                         long_description = configItems["long_description"],
                         author = configItems["author"], author_email = configItems["author_email"],
                         license = configItems["license"],
                         url = configItems["url"], packages = configItems["packages"],
                         package_dir = configItems["package_dir"],
                         data_files = configItems["data_files"],
                         ext_modules = configItems["ext_modules"])
