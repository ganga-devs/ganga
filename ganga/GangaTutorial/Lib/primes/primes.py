#!/usr/bin/env ganga

from __future__ import print_function

# Ganga tutorial application developed by Kuba Moscicki, CERN, 2006

# Helper function for prime number factorization application.

def check_prime_job(j):
    """ Check the output of job j (or subjobs if exist) and print all calculated prime factors.
    """
    NUMBER = j.application.number

    print('Looking for prime factors of',NUMBER)
    
    # we can use our function for subjobs or simple jobs at the same time
    if j.subjobs:
        rjobs = j.subjobs
    else:
        rjobs = [j]

    # loop over output files and build the factor tuples
    factors = []
    for s in rjobs:
        try:
            fc = eval(open(s.outputdir+'/factors-%d.dat'%NUMBER).read())
            if fc:
                print('job',s.id,': got factors:',fc)
            factors.extend(fc)
        except IOError as x:
            print(x)

    # check if we have all prime numbers!
    import math
    check = 1
    for f in factors:
            check *= int(math.pow(f[0],f[1]))
    print
    
    if check == NUMBER:
        print('All prime factors found:',factors)
    else:
        print('Factors found so far',factors,'mulitply to',check)
        print('There are still missing factors!')


def split_prime_job(j,millions,number=None):
    """ Create a splitter for job j to search a given number of millions of prime numbers.
    A number to factorize may be specified else the one defined before is taken. This is useful if you make a copy of a job.
    Example: split_prime_job(j,5,123456) - search first 5 millions of prime numbers and find factors of 123456
    """

    if not number:
        number = j.application.args[0]

    from GangaCore.GPI import File, TUTDIR, ArgSplitter, Executable
    j.application.exe=File(TUTDIR+'/Lib/primes/prime_factor.py')

    # make sure that we crab the data files!
    j.outputsandbox = ['*.dat']

    j.application.args = [str(number)]
    j.splitter = ArgSplitter()
    args = []
    for m in range(1,millions+1):
        args.append([str(number),'https://primes.utm.edu/lists/small/millions/primes%d.zip'%m])
    j.splitter.args = args
