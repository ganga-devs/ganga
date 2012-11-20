#! /usr/bin/env python

import getopt, sys, os, commands, gzip, time, re
import cPickle as pickle

def is_info(filter,attributes=[]):
    'Extract info from BDII'

    cmd = "ldapsearch -LLL -h %s -b 'mds-vo-name=local, o=grid' -x '%s' %s" % (bdii,filter,' '.join(attributes))
    rc, output = commands.getstatusoutput(cmd)
    
    result = []
    for line in output.splitlines():
        if not line: continue
        if line[0] == ' ':
            result[-1] += line[1:]
            continue
        result.append(line)

    return result

def getCEs(vo):
    'Retrieve CE info from BDII'

    re_result = re.compile('dn: GlueCEUniqueID=([\w\-\.:/_]+),[mM]ds-[Vv]o-name=([\w\-\.]+),mds-vo-name=([\w\-\.]+).*') 
    ces = []
    sites = {}
    for line in is_info('(&(objectclass=GlueCE)(GlueCEAccessControlBaseRule=vo:%s))' % vo,['GlueCEUniqueID']):
        if not line.startswith('dn:'): continue
        match = re_result.match(line)
        if not match:
            print 'ERROR Cannot match %s' % line
            continue
        ces.append(match.group(1))
        sites[match.group(1)] = match.group(2)

    return ces, sites

def getSEs(vo):
    'Retrieve SE info from DBII'

    result = []
    for line in is_info('(&(objectclass=GlueSA)(GlueSAAccessControlBaseRule=%s))' % vo,['GlueChunkKey']):
        if line.startswith('GlueChunkKey: GlueSEUniqueID='):
            result.append(line[29:])

    return result

def getCloseSEs(ce):
    'Retrieve CESE Association from BDII'

    result = []
    for line in is_info('(&(objectClass=GlueCESEBindGroup)(GlueCESEBindGroupCEUniqueID=%s))' % ce,['GlueCESEBindGroupSEUniqueID']):
        if line.startswith('GlueCESEBindGroupSEUniqueID:'):
            result.append(line[29:])

    return result

def purge_logfiles(directory,pattern,max_files):
    '''Purge logfiles'''

    max_index = 0
    re_name = re.compile(pattern)
    files = {}
    for name in os.listdir(directory):
        match = re_name.match(name)
        if match:
            index = int(match.group(1))
            files[index] = name
            if index > max_index: max_index = index

    for index, name in files.iteritems():
        if index <= (max_index - max_files + 1):
            os.unlink(os.path.join(directory,name))

    return max_index

def load_data(datafile):

    if os.path.exists(datafile):
        input = gzip.open(datafile,'rb')
        try:
            cese_time = pickle.load(input)
            ce_info = pickle.load(input)
            se_info = pickle.load(input)
            site_info = pickle.load(input)
            try:
                blacklist = pickle.load(input)
            except EOFError:
                blacklist = []
            return ce_info, se_info, site_info, blacklist
        except Exception:
            print >>log, 'Datafile %s was corrupted and will be regenerated.' % datafile

    return {}, {}, {}

def save_data(datafile,ce_info,se_info,site_info,blacklist_site):

    output = gzip.open(datafile,'wb')
    pickle.dump(time.time(),output)
    pickle.dump(ce_info,output)
    pickle.dump(se_info,output)
    pickle.dump(site_info,output)
    pickle.dump(blacklist_site,output)
    output.close()

def print_data(log,ce_info,se_info,site_info,blacklist_site):

    print >>log
    print >>log, 'Computing Elements'
    print >>log

    for ce, info in ce_info.iteritems():
        print >>log, ce
        print >>log, '    close_se: %s' % ',\n              '.join(info['close_se'])
        print >>log

    print >>log
    print >>log, 'Storage Elements'
    print >>log

    for se, info in se_info.iteritems():
        print >>log, se
        print >>log, '    close_ce: %s' % ',\n              '.join(info['close_ce'])
        print >>log

    print >>log
    print >>log, 'Site info'
    print >>log

    for site, ces in site_info.iteritems():
        print >>log, site
        print >>log, '    ce: %s' % ',\n        '.join(ces)


    print >>log
    print >>log, 'Blacklist Site'
    print >>log
    for site in blacklist_site:
        print >>log,'    %s' % site

if __name__ == '__main__':

    try:
        bdii = os.environ['LCG_GFAL_INFOSYS']
    except KeyError:
        bdii = 'lcg-bdii.cern.ch:2170'

    vo = 'atlas'
    max_missing = 2
    logfiles = None
    datafile = 'ce_se_info.data.gz'
    exclude_ce = []
    exclude_se = []
    blacklist_site = []

#   parse options 
    try:
        opts, args = getopt.getopt(sys.argv[1:],'',['vo=','datafile=','logfiles=','exclude-ce=','exclude-se=','blacklist-site='])
    except getopt.GetoptError, e:
        print >>sys.stderr, 'ERROR: %s' % e
        sys.exit(1)

    for opt, val in opts:

        if opt == '--vo':
            vo = val
            continue

        if opt == '--datafile':
            datafile = val
            continue

        if opt == '--logfiles':
            logfiles = val
            continue

        if opt == '--exclude-ce':
            exclude_ce.append(val)
            continue

        if opt == '--exclude-se':
            exclude_se.append(val)
            continue

        if opt == '--blacklist-site':
            blacklist_site += val.split(':') 
            continue

        print >>sys.stderr, 'ERROR: Unknown Option %s' % opt
        sys.exit()

#   purge old logfiles

    if logfiles:
        max_index = purge_logfiles(logfiles,'update-cese-(\d+).log.gz',5)
        log = gzip.open(os.path.join(logfiles,'update-cese-%6.6d.log.gz' % (max_index + 1)),'wb')
    else:
        log = sys.stdout

#   load information from the database

    ce_info, se_info, site_info, blacklist = load_data(datafile)

#   retrieve information on CEs and SEs from IS

    ce_list, ce_site = getCEs(vo)
    se_list = getSEs(vo)

    cese_assoc = {}
    for ce in ce_list:
        cese_assoc[ce] = getCloseSEs(ce)

#   convert everythiong lower case

    ce_list   = [ ce.lower() for ce in ce_list ]
    se_list   = [ se.lower() for se in se_list ]

    cese_tmp = {}
    for ce, close_se in cese_assoc.iteritems():
        cese_tmp[ce.lower()] = [ se.lower() for se in close_se ]
    cese_assoc = cese_tmp

    ce_site = dict( [ (ce.lower(), site.upper()) for ce, site in ce_site.iteritems() ] )

#   remove excluded CEs and SEs

    re_ce = re.compile('([\w.]+)(:\d+)?(/.*)')
    for ce in list(ce_list):
        match = re_ce.match(ce)
        if not match: continue
        if match.group(1) in exclude_ce:
            print >>log, 'Computing element %s has been excluded.' % ce
            ce_list.remove(ce)

#   remove excluded SEs

    for se in exclude_se:
        try:
            se_list.remove(se)
            print >>log, 'Storage Element %s has been excluded.' % se
        except ValueError:
            pass

#   look for missing ces and ses

    for ce, info in ce_info.items():
        if ce in ce_list: continue
        info['missing'] += 1
        if info['missing'] > max_missing:
            print >>log, 'Computing Element %s is missing since %d iterations and will be removed.' % (ce,info['missing'])
            del ce_info[ce]
        else:
            print >>log, 'Computing Element %s is missing since %d iterations.' % (ce,info['missing']) 

    for se, info in se_info.items():
        if se in se_list: continue
        info['missing'] += 1
        if info['missing'] > max_missing:
            print >>log, 'Storage Element %s is missing since %d iterations and will be removed.' % (se,info['missing'])
            del se_info[se]
        else:
            print >>log, 'Storage Element %s is missing since %d iterations.' % (se,info['missing'])

#   refresh info for existing ses and ces

    site_info = {}
    for ce in ce_list:
        if not ce_info.has_key(ce):
            print >>log, 'Computing Element %s has been added' % ce
        ce_info[ce] = {
            'close_se' : [],
            'missing' : 0
        }
        site = ce_site[ce]
        if site_info.has_key(site):
            if not ce in site_info[site]:
                site_info[site].append(ce)
        else:
            site_info[site] = [ce]

    for se in se_list:
        if not se_info.has_key(se):
            print >>log, 'Storage Element %s has been added' % se 
        se_info[se] = {
            'close_ce' : [],
            'missing' : 0
        }

    for ce in ce_list:
        for se in cese_assoc[ce]:
            if not se in se_list: continue
            if not se in ce_info[ce]['close_se']:
                ce_info[ce]['close_se'].append(se)
            if not ce in se_info[se]['close_ce']:
                se_info[se]['close_ce'].append(ce)

#   relink the missing ces and ses

    for ce, info in ce_info.iteritems():
        if not info['missing']: continue
        for se in info['close_se']:
            try:
                if not ce in se_info[se]['close_ce']:
                    se_info[se]['close_ce'].append(ce)
            except KeyError:
                pass

    for se, info in se_info.iteritems():
        if not info['missing']: continue
        for ce in info['close_ce']:
            if not ce_info.has_key(ce): continue
            if not se in ce_info[ce]['close_se']:
                ce_info[ce]['close_se'].append(se)

#   save output in the database

    save_data(datafile,ce_info,se_info,site_info,blacklist_site)

#   print information for humans

    print_data(log,ce_info,se_info,site_info,blacklist_site)

