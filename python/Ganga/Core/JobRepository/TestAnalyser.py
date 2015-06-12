##!/usr/bin/env python
################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestAnalyser.py,v 1.1 2008-07-17 16:40:50 moscicki Exp $
################################################################################

from __future__ import print_function

import os, sys
import re

##tests = ['register',  'checkout(10)', 'checkout(All)', 'commit',
##         'setStatus', 'getStatus(10)', 'getId', 'delete']
tests = ['register',  'checkout', 'commit', 'delete']

################################################################################    
def analyse(output_dir):
    pps = '-s->(.*)<-s-'
    ppj = 'users_(.*)__jobs_(.*)__subjobs_(.*)'
    m = re.search(ppj, output_dir)
    if m:
        nn    = int(m.group(1))
        njobs = int(m.group(2))
        sjobs = int(m.group(3))
    else:
        nn    = 1
        njobs = 1
        sjobs = 0

    ddd = {}
    for t in tests:
        ddd[t] = 0.
        
    files = os.listdir(output_dir)
    lf = len(files)
    for f in files:
        if f == 'analysis.txt':
            continue
        ff = file(os.path.join(output_dir, f), 'r')
        ll = ff.readlines()
        ff.close()
        itr = iter(tests)
        for l in ll:
            m = re.search(pps, l)
            if m:
                 ddd[next(itr)]+=float(m.group(1))/lf
    for k in ddd:
        ddd[k] = (ddd[k]/nn, ddd[k]/nn/njobs, ddd[k]/nn/njobs/(sjobs + 1))
    ddd['users'] = nn
    ddd['jobs']  = njobs
    ddd['sjobs'] = sjobs
    return ddd

################################################################################    
def compare(x, y):
    xn = x['users']
    xj = x['jobs']
    xs = x['sjobs']
    yn = y['users']
    yj = y['jobs']
    ys = y['sjobs']
    if xn < yn: return -1
    elif xn == yn:
        if xj < yj: return -1
        elif xj == yj:
            if xs < ys: return -1
            elif xs == ys: return 0
            else: return 1 
        else: return 1
    else: return 1
    
################################################################################
if __name__ == '__main__':
    OUTPUT = raw_input('Enter a name of output dir --->')
    output_dir = os.getcwd() 
    output_dir = os.path.join(output_dir, OUTPUT)
    ldict = []
    for dd in os.listdir(output_dir):
        if dd.startswith('users_'):
            analysis_dir = os.path.join(output_dir, dd)
            print("analysing %s ...\n" % analysis_dir)
            ddd = analyse(analysis_dir)
            ldict.append(ddd)
    
    ldict.sort(compare)
    
    str1 = "users    jobs    subjobs"
    str2 = "%4d      %4d    %4d   "
    for t in tests:
        str1 += t.rjust(45)
        str2 += "     (%10.6f    %10.6f    %10.6f)"
    str1 += "\n"
    str2 += "\n"

    f = file(os.path.join(output_dir, "analysis.txt"), "w")
    try:
        f.write(str1)
        
        for ddd in ldict:
            tt = [ddd['users'], ddd['jobs'], ddd['sjobs']]
            for t in tests:
                tt.extend(ddd[t])   
            f.write(str2 % tuple(tt))
    finally:
        f.close()

        





            
