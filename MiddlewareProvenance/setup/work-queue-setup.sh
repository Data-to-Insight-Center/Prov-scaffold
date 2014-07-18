#!/bin/sh
function usage
{
        echo 
        echo "#########################################"
        echo "#            work-queue-setup.sh             #"
        echo "#########################################"
        echo
        echo "$ work-queue-setup.sh <properties_file>"
                echo
}

if [ "$1" = "" ];
then
    usage;
    exit 1
fi

homePath=$(sed -n 's/.*workQueue_HOME *= *\([^ ]*.*\)/\1/p' < $1)
tmpPath=$(sed -n 's/.*workQueue_TMP *= *\([^ ]*.*\)/\1/p' < $1)
outputPath=$(sed -n 's/.*outputPath *= *\([^ ]*.*\)/\1/p' < $1)
mergePath=$(sed -n 's/.*mergePath *= *\([^ ]*.*\)/\1/p' < $1)
logPath=$(sed -n 's/.*logPath *= *\([^ ]*.*\)/\1/p' < $1)

if [ ! -d $tmpPath ]; then
  mkdir -p $tmpPath
fi

if [ ! -d $outputPath ]; then
  mkdir -p $outputPath
fi

if [ ! -d $mergePath ]; then
  mkdir -p $mergePath
fi

if [ ! -d $logPath ]; then
  mkdir -p $logPath
fi

if [ ! -d $logPath/worker_logs ]; then
  mkdir -p $logPath/worker_logs/
fi

rm -rf $tmpPath/*
rm -rf $outputPath/*
rm -rf $mergePath/*
rm $logPath/*.log
rm -rf $logPath/worker_logs/*

echo workQueue setup complete!
#mkdir notifications
