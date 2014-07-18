#!/bin/sh
function usage
{
        echo 
        echo "#########################################"
        echo "#            start-workers.sh             #"
        echo "#########################################"
        echo
        echo "$ start-workers.sh <properties_file>"
		echo
}

if [ "$1" = "" ];
then
    usage;
    exit 1
fi

noofworkers=$(sed -n 's/.*noOfWorkers *= *\([^ ]*.*\)/\1/p' < $1)
logPath=$(sed -n 's/.*logPath *= *\([^ ]*.*\)/\1/p' < $1)
for ((  i = 0 ;  i < $noofworkers;  i++  ))
do
  work_queue_worker -d all -t 100 -o $logPath/worker_logs/worker$i.log localhost 9123 &
done


