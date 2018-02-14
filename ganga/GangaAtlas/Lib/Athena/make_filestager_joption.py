#! /usr/bin/env python
###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: make_filestager_joption.py,v 1.8 2009-07-21 13:16:42 hclee Exp $
###############################################################################
# making input job option file for FileStager

import os
import os.path
import sys
import re
import pickle
import dm_util
import time

io_type = os.environ['DATASETTYPE']
io_mode = 'local'

if 'FILE_STAGER_IOMODE' in os.environ:
    io_mode = os.environ[ 'FILE_STAGER_IOMODE' ];

dq2tracertime = []
dq2tracertime.append(time.time())

## get GUID list from input_guids
guids = []
if os.path.exists('input_guids'):
    f = open('input_guids','r')
    for l in f.readlines():
        guids.append(l.strip())

## get dataset locations from os.environ['DATASETLOCATION']:
ds_locations = []
if 'DATASETLOCATION' in os.environ:
    ds_locations = os.environ['DATASETLOCATION'].split(':')

if (io_type in ['FILE_STAGER']):
    # determin site domain
    domain_replacements = {'grika.de': 'fzk.de'}
    site_domain = dm_util.get_site_domain(domain_replacements)
    print >> sys.stdout, 'detected site domain: %s' % site_domain

    # determin close se and its supported transfer protocols
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

    if not dq2_site_id and 'DQ2_LOCAL_SITE_ID' in os.environ:
        dq2_site_id = os.environ['DQ2_LOCAL_SITE_ID']

    print >> sys.stdout, 'detected DQ2_LOCAL_SITE_ID: %s' % dq2_site_id

    # get LFC_HOST associated with the dq2_site_id
    lfc_host = dm_util.get_lfc_host(dq2_site_id)
    print >> sys.stdout, 'LFC_HOST: %s' % lfc_host

    dq2tracertime.append(time.time())
    # resolve PFNs given the LFC_HOST and a list of GUIDs
    pfns,csum = dm_util.get_pfns(lfc_host, guids)
    dq2tracertime.append(time.time())

    #for guid in csum.keys():
    #    print >> sys.stdout, '%s %s:%s' % (guid, csum[guid]['csumtype'], csum[guid]['csumvalue'])

    # count only the PFNs on local site by match srm_endpoint of the dq2 site
    srm_endpt_info  = dm_util.get_srm_endpoint(dq2_site_id)
    print >> sys.stdout, str(srm_endpt_info)

    # define the default gridcopy protocol
    my_protocol = 'lcgcp'

    # check if DQ2_LOCAL_PROTOCOL is given: use the given protocol forcely
    if 'DQ2_LOCAL_PROTOCOL' in os.environ and os.environ['DQ2_LOCAL_PROTOCOL']:
        my_protocol = os.environ['DQ2_LOCAL_PROTOCOL']

        # just to convert the protocol name into FileStager convention
        if my_protocol in ['lcg-cp']:
            my_protocol = 'lcgcp'

    # try to detect the available protocol if io_mode is 'local'
    elif io_mode in [ 'local' ]:
        # determin the supported transfer protocols of the given SE
        protocols = dm_util.get_transfer_protocols(srm_endpt_info['se_host'])
        print >> sys.stdout, 'detected transfer protocols: %s' % repr(protocols)

        # chose a suitable protocol
        # 1. firstly remove gsiftp protocol (do we support it as an alternative of lcgcp?)
        # 2. pick up the first available protocol
        # 3. if no first protocol, use lcgcp instead
        try:
            protocols.remove('gsiftp')
        except ValueError:
            pass

        if protocols:

            ## make sure the 'file' protocl will be used if it's presented in the protocol list
            ##  - usually a case of Storm-based SE
            if 'file' in protocols:
                my_protocol = 'file'
            else:
                my_protocol = protocols[0]

    ## avoid using dcap or gsidcap until the load on SE with dcap is solved 
    #if my_protocol in ['dcap', 'gsidcap']:
    #    my_protocol = 'lcgcp'

    print >> sys.stdout, 'picked transfer protocol: %s' % my_protocol

    re_endpt = re.compile('^.*%s.*%s.*\s*$' % (srm_endpt_info['se_host'], srm_endpt_info['se_path']) )
    pfn_list = []
    pfn_csum = {}
    for guid in pfns.keys():
        print >> sys.stdout, 'guid:%s pfns:%s' % ( guid, repr(pfns[guid]) )
        for pfn in pfns[guid]:
            if re_endpt.match(pfn):
                pfn_list.append(pfn)
                pfn_csum[pfn] = csum[guid]

    # print out the PFNs and the checksum info. in LFC
    for pfn in pfn_csum.keys():
        print >> sys.stdout, '%s %s:%s' % (pfn, pfn_csum[pfn]['csumtype'], pfn_csum[pfn]['csumvalue'])
    # create a checksum list in pickle format
    fcsum = open('lfc_checksum.pickle','w')
    pickle.dump(pfn_csum, fcsum)
    fcsum.close()
    print >> sys.stdout, 'LFC checksum pickle: %s' % os.path.join(os.getcwd(), 'lfc_checksum.pickle')

    try:
        if 'ATHENA_MAX_EVENTS' in os.environ:
            evtmax = int(os.environ['ATHENA_MAX_EVENTS'])
        else:
            evtmax = -1
    except:
        evtmax = -1

    
    try:
        if 'ATHENA_SKIP_EVENTS' in os.environ:
                skipevt = int(os.environ['ATHENA_SKIP_EVENTS'])
        else:
                skipevt = 0
    except:
        skipevt = 0

    # produce the job option file for Athena/FileStager module
    fs_cp_cmd  = os.path.join( os.getcwd(), 'fs-copy.py' )
    fs_cp_args = []
    fs_of_prefix = 'file:'

    ic_jo_path = os.path.join(os.getcwd(), 'input.py')
    fs_jo_path = os.path.join(os.getcwd(), 'FileStager_jobOption.py')

    # sort pfn_list
    pfn_list.sort()

    dm_util.make_FileStager_jobOption(pfn_list, gridcopy=True, protocol=my_protocol, maxEvent=evtmax, skipEvent=skipevt, fs_cp_cmd=fs_cp_cmd, fs_cp_args=fs_cp_args, fs_of_prefix=fs_of_prefix, ic_jo_path=ic_jo_path, fs_jo_path=fs_jo_path)

    dq2tracertime.append(time.time())
    outFile = open('dq2tracertimes.txt','w')
    for itime in dq2tracertime:
        outFile.write('%s\n' % itime)
    outFile.close()


else:
    print >> sys.stderr, "make_filestager_joption.py supports only FILE_STAGER datasettype"
