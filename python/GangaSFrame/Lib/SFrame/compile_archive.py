#!/usr/bin/env python
##########################################################################
#
#  Script for extracting the analysis code source and compiling it
#  in a GANGA job.
#
##########################################################################

# Import the used modules:
import sys, os, string, re

#
# Collect the command line parameters:
#
if len( sys.argv ) != 2:
    print "Usage: %s <archive>" % sys.argv[ 0 ]
    sys.exit( 1 )

archive_file = sys.argv[ 1 ]
top_directory = os.environ['PWD']

#
# Decompress the archive in the specified directory:
#
print "Decompressing archive:"
decomp_command = "tar -xzvf " + archive_file + " -C " + top_directory
print "   %s" % decomp_command
sys.stdout.flush()
sys.stderr.flush()
os.system( decomp_command )

# Some global variables:
sframe_dir = []

def sframe_find( arg, dirname, fnames ):
    ''' Function for finding the main SFrame package. '''

    # Remove CVS directories from the search path:
    for i in range( fnames.count( "CVS" ) ):
        fnames.remove( "CVS" )

    if "SFrame" in os.path.basename( dirname ):
        sframe_package = False
        for fname in fnames:
            if fname == "Makefile":
                fl = os.path.join(dirname, fname)
                if not os.path.islink(fl):
                    sframe_dir.append( dirname )


# Find the SFrame sources:
#
print "Searching for the SFrame sources:"
os.path.walk( top_directory, sframe_find, None )
if len( sframe_dir ) < 1:
    print "   *** Error finding SFrame sources!!! --> Exiting. ***"
    sys.exit( 1 )
elif len( sframe_dir ) > 1:
    print "   *** Multiple SFrame sources!!! --> Exiting. ***"
    for i in sframe_dir:
        print "   dir: %s" % i
    sys.exit( 1 ) 
print "   SFrame source found under: %s" % sframe_dir[ 0 ]

#
# Set up the environment to compile the SFrame sources:
#

sframe_bin_path = os.path.join( sframe_dir[ 0 ], "bin" )
sframe_lib_path = os.path.join( sframe_dir[ 0 ], "lib" )

os.putenv( "SFRAME_DIR", sframe_dir[0] )
os.putenv( "SFRAME_BIN_PATH", sframe_bin_path )
os.putenv( "SFRAME_LIB_PATH", sframe_lib_path )

#
# Link JobConfig.dtd, lib and bin to top directory
#
os.system("ln -s %s/user/config/JobConfig.dtd %s" % (sframe_dir[0], top_directory))
#os.system("ln -s %s %s" % (sframe_bin_path, top_directory))
#os.system("ln -s %s %s" % (sframe_lib_path, top_directory))
    
#
# Compile the main SFrame library:
#
print "*"
print "*  Compiling the SFrame library and executable"
print "*"
sys.stdout.flush()
sys.stderr.flush()
os.chdir( sframe_dir[ 0 ] )
os.system( "gmake clean; gmake" )

#
# Print some good-bye message:
#
print ""
print ""
print "  Finished compiling analysis sources"
print ""
print "    SFrame executable is under: %s" % sframe_bin_path
print "    SFrame libraries are under: %s" % sframe_lib_path
print ""
print ""


# create file with SFRAME_DIR
fname = '%s/sfdir.tmp' % top_directory
of = open(fname, 'w')
of.write( sframe_dir[0] )
of.close()

sys.exit( 0 )
