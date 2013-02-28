#!/bin/bash

# wrapper to run the na62 mc generation script and pipe the output to the separate stdout/err
echo "Running: ${NA62SCRIPT} $1 $2 > ${NA62STDOUT} 2> ${NA62STDERR}"
chmod +x ${NA62SCRIPT}
./${NA62SCRIPT} $1 > ${NA62STDOUT} 2> ${NA62STDERR}
retcode=$?
echo "-------------------------------------------------"
echo $retcode
echo "-------------------------------------------------"
pwd
echo "-------------------------------------------------"
ls ../
echo "-------------------------------------------------"
ls ../../
echo "-------------------------------------------------"
ls
exit $retcode


