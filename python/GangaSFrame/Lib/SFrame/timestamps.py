#!/usr/bin/env python

import glob, re, time, sys

if len(sys.argv) > 1:

    lista = glob.glob("/afs/ifh.de/user/m/mbarison/gangadir/workspace/Local/"+sys.argv[1]+"/*/output/timestamps.txt")

    ptn_1 = re.compile(".*/([0-9]+)/[0-9]+/.*")
    ptn_2 = re.compile(".*/[0-9]+/([0-9]+)/.*")

    for i in lista:
        job   = ptn_1.findall(i)[0]
        sjob  = ptn_2.findall(i)[0]


        f = open(i)

        tuplelist = []

        for line in f.readlines():
            tp  = line.strip("(\n)").split(',')
            ntp = []
            
            for j in tp:
                #print j
                ntp += [int(j)]
            
            tuplelist.append(tuple(ntp))

        

        s2e = time.mktime(tuplelist[2])-time.mktime(tuplelist[0])
        r2e = time.mktime(tuplelist[2])-time.mktime(tuplelist[1])

        #s_days = s2e / (24*3600)
        s_hrs  = (s2e % (24*3600)) / 3600
        s_mnt  = (s2e % (3600)) / 60

        #r_days = r2e / (24*3600)
        r_hrs  = (r2e % (24*3600)) / 3600
        r_mnt  = (r2e % (3600)) / 60 

        print "Job %s Subjob %s S2E: %s R2E: %s" % \
              (job, sjob, \
               ("%d hours %d minutes" % (s_hrs, s_mnt)),
               ("%d hours %d minutes" % (r_hrs, r_mnt)))
        #       time.strftime("%d days %H hours %M minutes", timediff))

        f.close()

    sys.exit(0)

else:
    print "ERROR: input job number please"
    sys.exit(666)
