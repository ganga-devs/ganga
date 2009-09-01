#! /bin/sh -x


if [ -z $ATHENA_STDOUT ]
then
    export ATHENA_STDOUT='stdout.txt'
fi
if [ -z $ATHENA_STDERR ]
then
    export ATHENA_STDERR='stderr.txt'
fi

ls

echo "********* Running ATLAS code ******************"

./athena-ng.sh $@ 1> $ATHENA_STDOUT 2> $ATHENA_STDERR

echo "********* Done with ATLAS code ******************"

ls

gzip $ATHENA_STDOUT
gzip $ATHENA_STDERR

exit 0

