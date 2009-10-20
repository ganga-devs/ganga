## This file lists the set of Ganga externals. Is is simply executed by
## the lhcb-prepare script

# These are the external packages with only Python code.
externals_noarch = [['ApMon','2.2.11'],
                    ['figleaf', '0.6'],
                    ['paramiko', '1.7.3'],
                    ['PYTF','1.5'],
                    ['stomputil','2.0']]


# These are the packages with architecture dependent code.
externals_arch = [['matplotlib','0.99.0'],
                  ['numpy','1.3.0'],
                  ['pycrypto','2.0.1'],
                  ['pyqt','3.18.1_python2.5']]
archs = ['slc4_amd64_gcc34','slc4_ia32_gcc34','x86_64-slc5-gcc43-opt']
