#!/usr/bin/env python3

from __future__ import print_function

# Ganga tutorial application developed by Kuba Moscicki, CERN, 2006

# Find all prime factors of a given NUMBER using a list of primes stored in a text FILE.
# Prime factors are written to a *.dat file in the following way:
# [(p1,n1), (p2,n2), ..., (pk,nk)]
#
# p are prime factors and n are multipliers i.e. p1^n1 * p2^n2 * ... * pk^nk is a factor of the NUMBER
#
# Arguments: NUMBER FILE
#
# The FILE has the following format:
#                  The First 1,000,000 Primes (from primes.utm.edu)
#
#         2         3         5         7        11        13        17        19
#        23        29        31        37        41        43        47        53
#        59        61        67        71        73        79        83        89
#        97       101       103       107       109       113       127       131
#       137       139       149       151       157       163       167       173
#       179       181       191       193       197       199       211       223
#       227       229       233       239       241       251       257       263
#       269       271       277       281       283       293       307       311
#
# If URL (http address) is given, then file is downloaded and unzipped first.
#

import sys, os
import requests

NUMBER = int(sys.argv[1])

pfns = sys.argv[2:]

factors = []

currDir = os.path.dirname(os.path.abspath(__file__))

def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True, verify=False)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

n = NUMBER

for pfn in pfns:
    # download primes file from the web server, unzip and replace extension from .zip to .txt
    # print(pfn)
    if pfn.find('http://') != -1 or pfn.find('https://') != -1:
        download_url(pfn, os.path.join(currDir, os.path.basename(pfn)))
        pfn = os.path.basename(pfn)
        os.system('unzip %s'%pfn)
        pfn = os.path.splitext(pfn)[0]+'.txt'

    pf = open(pfn)

    # skip two first lines of the file
    pf.readline()
    pf.readline()

    line = pf.readline()

    # scan all lines in the file and try to divide the NUMBER by consecutive primes
    while line:
        primes = [int(x) for x in line.split()]

        for p in primes:
            k = 0
            while n%p == 0:
                k += 1
                n //= p

            if k>0:
                factors.append((p,k))
    
        line = pf.readline()

print('Prime factors:',factors)

# check if all prime factors have been found
import math
check = 1
for f in factors:
    for k in range(f[1]):
        check *= f[0]

if check == NUMBER:
    print('All prime factors found!')
else:
    print('Some prime factors are still to be found. Known factors multiply to',check)

# write the factors to a data file
ofn = 'factors.dat'
of = open(ofn,'w')
of.write(str(factors))
print('Created data file',ofn)
