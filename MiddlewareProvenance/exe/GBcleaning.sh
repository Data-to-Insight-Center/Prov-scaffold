#!/bin/sh
function usage
{
        echo 
        echo "#########################################"
        echo "#            GBcleaning.sh             #"
        echo "#########################################"
        echo
        echo "$ GBcleaning.sh <properties_file>"
                echo
}

if [ "$1" = "" ];
then
    usage;
    exit 1
fi

homePath=$(sed -n 's/.*workQueue_HOME *= *\([^ ]*.*\)/\1/p' < $1)
tmpPath=$(sed -n 's/.*workQueue_TMP *= *\([^ ]*.*\)/\1/p' < $1)

if [ -d $tmpPath ]; then
  rm -rf $tmpPath 
  echo $tmpPath removed...
fi

if [ -d $homePath ]; then
  rm $homePath/*.log
  echo temp logs removed...
  rm $homePath/*.makeflowlog
  echo makeflow log removed...
fi

echo workQueue complete-All garbage collected
#mkdir notifications
