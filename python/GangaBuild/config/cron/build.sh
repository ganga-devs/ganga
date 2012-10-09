#!/bin/bash
date

cd /home/dboard/dashboard/trunk/arda.dashboard

PIDFILE=/tmp/build.pid
BUILDFILE=/tmp/build.out

FORCE=
TYPE=
while getopts 'ft:' OPTION
	do
	  case $OPTION in
	  f)	FORCE=1
			;;
	  t)
	        TYPE="$OPTARG"
			;;
	  ?)	printf "Usage: %s: [-f] [-t value] args\n
Options:
  f: force the build (even if there was no svn update
  t <type>: create the <type> build (instead of stable and unstable)
" $(basename $0) >&2
			exit 2
			;;
	  esac
	done
	shift $(($OPTIND - 1))




if [ -f $PIDFILE ];
then
#    echo "The pidfile exists"
    PID=`cat $PIDFILE`
    kill -0 $PID 2> /dev/null
    if [ $? -eq 0 ];
    then
       echo "The process $PID is still there..."
       exit 0
    fi
fi

echo $$  > $PIDFILE


if [ "$FORCE" == "1" ];
then
  echo "We are forcing the build"
else
  echo "Checking if there is any update"
  svn update |grep '^U'
  if [ $? -ne  0 ];
  then
    echo "There is no update"
    date
    exit 0
  fi
fi
echo "NOW WE BUILD (output in $BUILDFILE $TYPE"
if [ "$TYPE" != "" ];
then
  python setup.py fullbuild  --skip-apt --build-file config/build/dashboard-$TYPE.xml -a /var/www/html/apt/ -d /var/www/html/build/$TYPE/ > $BUILDFILE 2>&1

else
  python setup.py fullbuild  --skip-apt --build-file config/build/dashboard-unstable.xml -a /var/www/html/apt/ -d /var/www/html/build/unstable/ > $BUILDFILE 2>&1

  python setup.py fullbuild  --skip-apt --build-file config/build/dashboard-stable.xml -a /var/www/html/apt/ -d /var/www/html/build/stable > $BUILDFILE 2>&1
fi

echo "BUILD IS DONE "
date
