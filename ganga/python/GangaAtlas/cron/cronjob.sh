#! /bin/sh

`dirname $0`/update_cese_info.py --vo atlas \
    --datafile='/afs/cern.ch/sw/ganga/www/ATLAS/cese_info.dat.gz' \
    --logfiles='/afs/cern.ch/sw/ganga/www/ATLAS' \
    --exclude-ce='cclcgceli04.in2p3.fr' \
    --exclude-ce='cclcgceli02.in2p3.fr' \
    --blacklist-site=IFAE:MANC:UCLHEP:UVIC:ASGCDISK:AU-ATLAS:TW-FTT
