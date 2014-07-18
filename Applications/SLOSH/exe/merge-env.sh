#!/bin/sh
function usage
{
        echo 
        echo "#########################################"
        echo "#            merge-env.sh             #"
        echo "#########################################"
        echo
        echo "$ merge-env.sh <properties_file>"
                echo
}

if [ "$1" = "" ];
then
    usage;
    exit 1
fi

tclPath=$(sed -n 's/.*tclExecutablePath *= *\([^ ]*.*\)/\1/p' < $1)
outputPath=$(sed -n 's/.*outputPath *= *\([^ ]*.*\)/\1/p' < $1)
mergePath=$(sed -n 's/.*mergePath *= *\([^ ]*.*\)/\1/p' < $1)

echo "Files to be merged"

outputList=`ls -a $outputPath`

for i in $outputList
do  
echo "$i"  
done 

echo "Begin to merge." 
start_time=`date +%s`
tclsh $tclPath $outputPath $mergePath
#tclsh MEOWgen_v2/meowgen_cmd_v2.2.tcl /home/quzhou/tomcat/tomcat_app/output  outputs/merge-output>>logs/merge.log
end_time=`date +%s`

#cd outputs/merge-output/

echo "Output of merge operation" 

mergeList=`ls -a $mergePath`
for i in $mergeList
do  
echo "$i"
done

echo start time "$start_time"
echo end time "$end_time"
echo execution time `expr $end_time - $start_time`
