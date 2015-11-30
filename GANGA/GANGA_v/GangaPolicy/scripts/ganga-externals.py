## This file lists the set of Ganga externals. Is is simply executed by
## the lhcb-prepare script

# These are the external packages with only Python code.
externals_noarch = [['httplib2', '0.8'],
                    ['python-gflags', '2.0'],
                    ['google-api-python-client', '1.1'],
                    ['paramiko', '1.7.3'],
                    ['stomputil','2.4'],
                    ['ipython', '1.2.1']]


# These are the packages with architecture dependent code.
externals_arch = [['pycrypto','2.0.1']]
archs = ['x86_64-slc6-gcc48-opt']


