#! /usr/bin/env python
###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: make_filestager_joption.py,v 1.2 2008-12-15 10:10:55 hclee Exp $
###############################################################################
# making input job option file for FileStager

import os
import os.path
import sys
import re
import dm_util

io_type = os.environ['DATASETTYPE']

## get GUID list from input_guids
guids = []
if os.path.exists('input_guids'):
    f = open('input_guids','r')
    for l in f.readlines():
        guids.append(l.strip())

## get dataset locations from os.environ['DATASETLOCATION']:
ds_locations = []
if os.environ.has_key('DATASETLOCATION'):
    ds_locations = os.environ['DATASETLOCATION'].split(':')

if (io_type in ['FILE_STAGER']):
    # determin site domain
    domain_replacements = {'grika.de': 'fzk.de'}
    site_domain = dm_util.get_site_domain(domain_replacements)
    print >> sys.stdout, 'detected site domain: %s' % site_domain

    # determin close se
    se_replacements = {'srm.cern.ch': 'srm-atlas.cern.ch'}
    close_se = dm_util.get_se_hostname(se_replacements)
    print >> sys.stdout, 'detected closed SE: %s' % close_se

    # resolve the dq2_local_site_id taking into account
    #  - file locations
    #  - site_domain
    #  - se_hostname
    #  - site configurations (for some special sites)

    # define a list of possible site ids for combined sara-nikhef site
    # allowing jobs at NIKHEF to read data from SARA and vice versa
    nl_sites = dm_util.get_srmv2_sites(cloud='NL')
    sara_nikhef_combine = []
    for s in nl_sites:
        if ( s.find('SARA-MATRIX') >= 0 ) or ( s.find('NIKHEF-ELPROD') >= 0 ):
            sara_nikhef_combine.append(s)

    force_siteid_domain = {'.*nikhef.nl.*': sara_nikhef_combine,
                           '.*sara.nl.*'  : sara_nikhef_combine}

    force_siteid_se = {'srm-disk.pic.es': ['PIC_DATADISK']}

    dq2_site_id = dm_util.resolve_dq2_local_site_id(ds_locations, site_domain, close_se, \
                                force_siteid_domain=force_siteid_domain, \
                                force_siteid_se=force_siteid_se)

    print >> sys.stdout, 'detected DQ2_LOCAL_SITE_ID: %s' % dq2_site_id

    # get LFC_HOST associated with the dq2_site_id
    lfc_host = dm_util.get_lfc_host(dq2_site_id)
    print >> sys.stdout, 'LFC_HOST: %s' % lfc_host

    # resolve PFNs given the LFC_HOST and a list of GUIDs
    pfns = dm_util.get_pfns(lfc_host, guids)

    # count only the PFNs on local site by match srm_endpoint of the dq2 site
    srm_endpt_info  = dm_util.get_srm_endpoint(dq2_site_id)
    print >> sys.stdout, str(srm_endpt_info)
    re_endpt = re.compile('^.*%s.*%s.*\s*$' % (srm_endpt_info['se_host'], srm_endpt_info['se_path']) )
    pfn_list = []
    for guid in pfns.keys():
        print >> sys.stdout, 'guid:%s pfns:%s' % ( guid, repr(pfns[guid]) )
        for pfn in pfns[guid]:
            if re_endpt.match(pfn):
                pfn_list.append(pfn)

    print >> sys.stdout, str(pfn_list)

    try:
        if os.environ.has_key('ATHENA_MAX_EVENTS'):
            evtmax = int(os.environ['ATHENA_MAX_EVENTS'])
        else:
            evtmax = -1
    except:
        evtmax = -1

    # produce the job option file for Athena/FileStager module
    dm_util.make_FileStager_jobOption(pfn_list, gridcopy=True, maxEvent=evtmax, optionFileName='input.py')

else:
    print >> sys.stderr, "make_filestager_joption.py supports only FILE_STAGER datasettype"
