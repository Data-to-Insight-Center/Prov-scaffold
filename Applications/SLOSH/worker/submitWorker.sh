#! /bin/bash
module load futuregrid
module load python/2.7
module load gcc
source ~/env.sh

function usage
{
        echo
        echo "#########################################"
        echo "#            submitWorker.sh             #"
        echo "#########################################"
        echo
        echo "$ submitWorker.sh <hostname> <port> <worker_number>"
                echo
}

if [ "$1" = "" ];
then
    usage;
    exit 1
fi

if [ "$2" = "" ];
then
    usage;
    exit 1
fi

if [ "$3" = "" ];
then
    usage;
    exit 1
fi

work_queue_worker -t 90 -d all -o ~/worker_logs/worker$3.log $1 $2 &
