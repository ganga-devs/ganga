#!/usr/bin/env python
import glob, sys, os, getopt, shutil, string
from subprocess import call

"""
Default distutils 'setup' method overwritten.
"""

def usage():
    print '''
usage: package-builder VERSION

Tool to build Ganga RPMs and/or Python Eggs.

-h, --help
    Display this usage guide.

-b  --builddir
    Location in which to perform RPM builds
    Default: /tmp/gangabuild

-v, --version 
    Version of ganga we're building. This will be checked out of
    SVN and into [builddir]. Assumed to be available at 
    svn.cern.ch/reps/ganga/tags/Ganga-n-n-n<version>
    Example: 6.0.0 will retrieve tags/Ganga-6-0-0 (note the automatic
    conversion of '.' to '-'.
'''

try:
    options, args = getopt.getopt(sys.argv[1:], "hv:", ["help", "version="])
except getopt.error, x:
    print "command line syntax error"
    usage()
    sys.exit(2)

this_version = None
builddir = None
for opt, arg in options:
    if opt in ('-h', '--help'):
        usage()
        sys.exit(2)
    elif opt in ('-b', '--builddir'):
        builddir = arg
    elif opt in ('-v', '--version'):
        this_version = arg 
    else:
        usage()
        sys.exit(2)

if this_version == None:
   print "Missing [-v/--version] parameter"
   usage()
   sys.exit(2)

if builddir == None:
   builddir = '/tmp/gangabuild'
else:
   print "Missing builddir option"
   usage()
   sys.exit()

workdir = os.path.join(builddir, 'workspace')

if os.path.isdir(builddir):
   shutil.rmtree(builddir)
   os.makedirs(workdir)
else:
   os.makedirs(workdir)

topdir = 'Ganga-' + string.replace(this_version, '.', '-')

print "Attempting to export Ganga " + this_version + " from SVN."
os.chdir(builddir)
svnurl = 'svn+ssh://svn.cern.ch/reps/ganga/tags/' + topdir
svncmd = 'svn export -q ' + svnurl
exitcode = call([svncmd], shell=True)
if exitcode != 0:
   print "Problem exporting Ganga from SVN. Exiting."
   sys.exit()
else:
   print "Successfully exported Ganga " + this_version + " from SVN."
   os.makedirs(builddir + '/' + topdir + '/python/GangaBin')

rpm_script = '''
rm -rf $RPM_INSTALL_PREFIX/python/%{name}*egg*
if [ \"%{name}\" == "GangaBin" ]
then
  mv $RPM_INSTALL_PREFIX/python/%{name}/* $RPM_INSTALL_PREFIX/python/
  rm -rf $RPM_INSTALL_PREFIX/python/%{name}
  rm -rf $RPM_INSTALL_PREFIX/python/__init__.py
fi
'''

rpm_postinfile = open(builddir +'/postin-packages.sh','w')
print "Writing " + rpm_postinfile.name
rpm_postinfile.write(rpm_script)
rpm_postinfile.close()

rpm_script = '''
echo "Uninstalling" %{name}

if [ \"%{name}\" == "GangaBin" ]
then
  rm -rf $RPM_INSTALL_PREFIX/python/bin
  rm -rf $RPM_INSTALL_PREFIX/python/LICENSE_GPL
  rm -rf $RPM_INSTALL_PREFIX/python/release
  rm -rf $RPM_INSTALL_PREFIX/python/doc
  rm -rf $RPM_INSTALL_PREFIX/python/templates
else
  #echo "Deleting" $RPM_INSTALL_PREFIX/python/%{name}
  rm -rf $RPM_INSTALL_PREFIX/python/%{name}
fi
  
if [ ! "$(ls -A $RPM_INSTALL_PREFIX/python)" ]; then
  echo "$RPM_INSTALL_PREFIX/python is empty...removing."
  rm -rf $RPM_INSTALL_PREFIX/python
fi

if [ ! "$(ls -A $RPM_INSTALL_PREFIX)" ]; then
  echo "$RPM_INSTALL_PREFIX is empty...removing."
  rm -rf $RPM_INSTALL_PREFIX
fi
'''

rpm_postfile = open(builddir +'/postun-packages.sh','w')
print "Writing " + rpm_postfile.name
rpm_postfile.write(rpm_script)
rpm_postfile.close()

#create a manifest template because we need to force the inclusion of
#all files in the rpm including non-python files.
man_script = '''global-include *
''' 

man_file = open(builddir + '/MANIFEST.in','w')
print "Writing " + man_file.name
man_file.write(man_script)
man_file.close()

abstopdir = os.path.join(builddir, topdir)

#copy this set of directories into a fake package directory so we can package them all up together
#this could probably be much more elegant.
shutil.copytree(str(abstopdir)+'/bin', abstopdir + '/python/GangaBin/bin')
#omit the doc directory, as this invokes complicated perl requirements
shutil.copytree(str(abstopdir)+'/doc', abstopdir + '/python/GangaBin/doc')
shutil.copytree(str(abstopdir)+'/release', abstopdir + '/python/GangaBin/release')
shutil.copytree(str(abstopdir)+'/templates', abstopdir + '/python/GangaBin/templates')
shutil.copy(str(abstopdir)+'/LICENSE_GPL', abstopdir + '/python/GangaBin')
#we need to trick distutils into thinking GangaBin is a real python package
#for this we need to add a __init__.py file
#this should then be removed with the post_install script
trickfile = open(abstopdir + '/python/GangaBin/__init__.py','w')
trickfile.close()

print abstopdir
packageDirs = glob.glob(abstopdir + '/python/Ganga*')

rpm_require_map = {
'GangaBin' : "python >= 2.4.3, Ganga >= "+this_version,
'Ganga' : "GangaBin >= "+this_version,
'GangaAtlas' : "Ganga >= "+this_version,
'GangaCamtology' : "Ganga >= "+this_version,
'GangaCMS' : "Ganga >= "+this_version,
'GangaDirac' : "Ganga >= "+this_version,
'GangaGaudi' : "Ganga >= "+this_version,
'GangaLHCb' : "Ganga >= "+this_version,
'GangaNA62' : "Ganga >= "+this_version,
'GangaPanda' : "Ganga >= "+this_version,
'GangaPlotter' : "Ganga >= "+this_version,
'GangaRobot' : "Ganga >= "+this_version,
'GangaSAGA' : "Ganga >= "+this_version,
'GangaService' : "Ganga >= "+this_version,
'GangaSuperB' : "Ganga >= "+this_version,
'GangaTest' : "Ganga >= "+this_version,
'GangaTutorial' : "Ganga >= "+this_version
}

egg_require_map = {
'GangaBin' : ["python>=2.4.3"],
'Ganga' : ["GangaBin=="+this_version],
'GangaAtlas' : ["Ganga=="+this_version],
'GangaCamtology' : ["Ganga=="+this_version],
'GangaCMS' : ["Ganga=="+this_version],
'GangaDirac' : ["Ganga=="+this_version],
'GangaGaudi' : ["Ganga=="+this_version],
'GangaLHCb' : ["Ganga=="+this_version],
'GangaNA62' : ["Ganga=="+this_version],
'GangaPanda' : ["Ganga=="+this_version],
'GangaPlotter' : ["Ganga=="+this_version],
'GangaRobot' : ["Ganga=="+this_version],
'GangaSAGA' : ["Ganga=="+this_version],
'GangaService' : ["Ganga=="+this_version],
'GangaSuperB' : ["Ganga=="+this_version],
'GangaTest' : ["Ganga=="+this_version],
'GangaTutorial' : ["Ganga=="+this_version]
}

description_map = {
'Ganga' : 'The Core Ganga package', 
'GangaBin' : 'Contains the Ganga executable, release scripts, documents and templates', 
'GangaAtlas' : 'The Ganga ATLAS package',
'GangaCamtology' : 'The Ganga Camtology package',
'GangaCMS' : 'The Ganga CMS package',
'GangaDirac' : 'The Ganga Dirac package',
'GangaGaudi' : 'The Ganga Gaudi package',
'GangaLHCb' : 'The Ganga LHCb package',
'GangaNA62' : 'The Ganga NA62 package',
'GangaPanda' : 'The Ganga Panda package',
'GangaPlotter' : 'The Ganga Plotter package',
'GangaRobot' : 'The Ganga Robot package',
'GangaSAGA' : 'The Ganga SAGA package',
'GangaService' : 'The Ganga Service package',
'GangaSuperB' : 'The Ganga SuperB package',
'GangaTest' : 'The Ganga Testing package',
'GangaTutorial' : 'The Ganga Tutorial package'
}

long_desc_map = {}

os.chdir(workdir)

summaryDict = {}
for package in packageDirs:
    os.chdir(workdir)
    pack = str(os.path.basename(package))
    print "################################################################"
    print "Working on package " + pack 
    print "################################################################"

    fullpath = os.path.join(abstopdir+'/python/'+pack)
    print str(fullpath)
    print str(pack)
    shutil.copytree(str(fullpath), './' + str(pack))
#    if pack == 'GangaBin':
#        os.chdir('GangaBin')
    shutil.copy(builddir+'/MANIFEST.in', workdir)

    config_script = '''[global]
verbose         = 1
force-manifest  = 0

[sdist]
dist-dir        = /afs/cern.ch/sw/ganga/www/download/repo/src

[bdist]
dist-dir        = /afs/cern.ch/sw/ganga/www/download/repo/bin
plat-name       = noarch

[bdist_rpm]
dist-dir = /afs/cern.ch/sw/ganga/www/download/repo/NOARCH
vendor = "Ganga <project-ganga-developers@cern.ch>"
###REQUIREMENTS###

[install]
prefix = /opt/ganga/install/python
install_lib     = /opt/ganga/install/python
compile         = 0
'''

    config_script = config_script.replace('###REQUIREMENTS###', 'requires = ' + rpm_require_map[pack])
    print config_script
    conf_file = open('setup.cfg','w')
    print "Writing " + conf_file.name
    conf_file.write(config_script)
    conf_file.close()

    setup_script = '''#!/usr/bin/env python
import glob
import sys
import os
#Removing os.link() gets around the fact that hardlinks across directories arent supported by AFS
#See http://qwone.com/~jason/python/
del os.link
from setuptools import setup, find_packages

setup(
        #find all of the pythonic files
        packages = find_packages(),
        #and also things other than *.py, e.g. *.gpi, *.gpim etc
        include_package_data = True,

        #installation requirements relating to the egg distribution
        ###REQUIREMENTS###

        ###PACKAGENAME###

        ###THISVERSION###

        ###DESCRIPTION###
        description = 'Description goes here',

        ###LONG_DESCRIPTION###
        long_description = "Long description goes here",

        url = "http://ganga.web.cern.ch/ganga/",
        author = "The Ganga Project",
        author_email = "project-ganga-developers@cern.ch"
    )
'''

    filenames = []
    
#    setup_script = setup_script.replace('###PACKAGES###', 'packages = ' + str([pack])+',')
    setup_script = setup_script.replace('###REQUIREMENTS###', 'install_requires = ' + str(egg_require_map[pack])+',')
    setup_script = setup_script.replace('###PACKAGENAME###', 'name = \"' + pack + '\",')
    setup_script = setup_script.replace('###THISVERSION###', 'version = \"' + this_version+ '\",')

    setup_file = open('setup.py','w')
    print "Writing " + setup_file.name
    setup_file.write(setup_script)
    setup_file.close()

    #if call(["python setup.py --quiet bdist_rpm --post-uninstall " + abstopdir + "/release/tools/postun-packages.sh > /dev/null 2>&1"], shell=True, stdin = None, stdout = None) == 0:

    build_cmd = "python setup.py bdist_rpm "
    build_cmd += " --post-install " + builddir + "/postin-packages.sh "
    build_cmd += " --post-uninstall " + builddir + "/postun-packages.sh "
    build_cmd += " --no-autoreq "
    build_cmd += " &> " + builddir + "/" + pack + ".log"
    print build_cmd
    if call(build_cmd, shell=True) == 0:
        summaryDict[pack] = 'OK'
    else:
        summaryDict[pack] = 'Failure'
   
#    print "Moving " + str(pack) + " to " + str(fullpath)
#    shutil.copytree(str(pack), str(fullpath))
    shutil.rmtree(str(pack))
    os.chdir(builddir)
    print "Removing workspace"
    shutil.rmtree('workspace')
    print "Creating workspace"
    os.makedirs('workspace')

print "################################"
print "################################"
print "######## Build Summary #########"
print "################################"
print "################################"
for k in summaryDict.keys():
    print "%15s: %15s" % (k, summaryDict[k])
