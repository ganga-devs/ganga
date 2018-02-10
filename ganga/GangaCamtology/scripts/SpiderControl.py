#!/bin/env python

# -----------------------------------------------
# The master control program for setting up and working the spidering utilities
# -----------------------------------------------

import sys, os
from ConfigParser import ConfigParser

# -----------------------------------------------
# Globals
gConfig = ConfigParser()

# -----------------------------------------------
# Functions
def runSpider( ):

    # Prepare the spider
    linkdir = os.getcwd()
    domlist = 'no_domain_file'
    dailyhit = '1000'
    domsperjob = 5
    gangaexec = "/home/atsoft3/common/Ganga/install/5.1.2/bin.ganga"
    imageext = "jpg,jpeg"
    fileext = "pdf"
    safedomains = "ox.ac.uk"
    payloadoutput = ""
    addinputs = ""
    
    if gConfig.has_section('General'):
        if gConfig.has_option('General', 'LinkStorePath'):
            linkdir = gConfig.get('General', 'LinkStorePath')
        if gConfig.has_option('General', 'AdditionalInputs'):
            addinputs = gConfig.get('General', 'AdditionalInputs')
            
    if gConfig.has_section('Domain'):
        if gConfig.has_option('Domain', 'DomainList'):
            domlist = gConfig.get('Domain', 'DomainList')
        if gConfig.has_option('Domain', 'MaxLinksPerDay'):
            dailyhit = gConfig.get('Domain', 'MaxLinksPerDay')
        if gConfig.has_option('Domain', 'SafeDomains'):
            safedomains = gConfig.get('Domain', 'SafeDomains')

    if gConfig.has_section('Ganga'):
        if gConfig.has_option('Ganga', 'DomainsPerJob'):
            domsperjob = gConfig.get('Ganga', 'DomainsPerJob')
        if gConfig.has_option('Ganga', 'ExecPath'):
            gangaexec = gConfig.get('Ganga', 'ExecPath')

    if gConfig.has_section('Page'):
        if gConfig.has_option('Page', 'ImageExtensions'):
            imageext = gConfig.get('Page', 'ImageExtensions')
        if gConfig.has_option('Page', 'FileExtensions'):
            fileext = gConfig.get('Page', 'FileExtensions')
        if gConfig.has_option('Page', 'Payload_exe'):
            payload = gConfig.get('Page', 'Payload_exe')
        if gConfig.has_option('Page', 'Payload_output'):
            payloadoutput = gConfig.get('Page', 'Payload_output')

    os.system("mkdir -p " + linkdir)
    os.system("cp " + domlist + " " + os.path.join(linkdir, os.path.basename(domlist) ) )
    
    # create spider iteration script
    cmd = "export SPIDER_MAXDAILYHIT=" + dailyhit + " ; "
    cmd += "export SPIDER_DOMSPERJOB=" + domsperjob + " ; "
    cmd += "export SPIDER_DOMAINLISTFILE=" + os.path.join(linkdir, os.path.basename(domlist) ) + " ; "
    cmd += "export SPIDER_IMAGEEXTENSIONS=" + imageext + " ; "
    cmd += "export SPIDER_FILEEXTENSIONS=" + fileext + " ; "
    cmd += "export SPIDER_PAYLOAD=" + payload + " ; "
    cmd += "export SPIDER_SAFEDOMAINS=" + safedomains + " ; "
    cmd += "export SPIDER_LINKDIR=" + linkdir + " ; "
    cmd += "export SPIDER_PAYLOADOUTPUT=" + payloadoutput + " ; "
    cmd += "export SPIDER_ADDINPUTS=" + addinputs + " ; "
    cmd += "export GANGA_CONFIG_PATH=/disk/f8b/home/mws/camont/camont.ini ; "
    cmd += gangaexec + " "
    cmd += "-o'[Configuration]gangadir=" + os.path.join(linkdir, "gangadir") + "' "
    cmd += "-o'[LCG]Config=" + os.path.join(os.getcwd(), "glite_wmsui.conf") + "' "
    cmd += "-o'[LCG]VirtualOrganisation=camont' -o'[defaults_GridProxy]voms=camont'"
    cmd += os.path.join(os.getcwd(), "runSpider.py")
    
    print cmd
    
    os.system( cmd )
    
    return

# -----------------------------------------------
# main
if len(sys.argv) < 2:
    print "Please supply a config file"
    sys.exit(1)
elif len(sys.argv) > 2:
    print "Too many arguments. I just need a config file."
    sys.exit(1)

conf_file = sys.argv[1]

print "-----------------------------------------------"
print "Welcome to the Imense Spidering software"
print "\nUsing config file '" + conf_file + "'"
print "-----------------------------------------------"

# get the config
gConfig.read( conf_file )
runSpider()
print "All Done!"

sys.exit(0)
