#!/bin/bash

#OPTIONS:
#   -f      Force the recreation of the test repository. This will overwrite the existing repo, if it exists.
#   -d      Destination of repository (default: ~/gangadir_schema_test).
#   -v      Version of Ganga to execute from /afs/cern.ch/sw/ganga/install (e.g. 5.8.9-pre).
#   -r      Location of (temporary) Gangadir in which to create repository. If this exists, an attempt will always be made to delete it.
#   -h      Show this message.

for minorversion in `seq 0 17`   
do
/afs/cern.ch/sw/ganga/install/5.8.18-pre/release/tools/schema_gen.sh -g 5.8.18-pre -v 5.8.$minorversion-pre
done
