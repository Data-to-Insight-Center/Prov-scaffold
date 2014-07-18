#!/bin/sh
JAVA_HOME=
SLOSHKarma_HOME=/home/quzhou/SLOSHKarma
KarmaClient_HOME=/home/quzhou/KarmaClient

cd $KarmaClient_HOME
for i in $(ls lib |grep jar); do
	CLASSPATH=$CLASSPATH:$KarmaClient_HOME/lib/$i
done
for j in $(ls build/lib |grep jar); do
	CLASSPATH=$CLASSPATH:$KarmaClient_HOME/build/lib/$j
done

cd $SLOSHKarma_HOME

for i in $(ls lib |grep jar); do
	CLASSPATH=$CLASSPATH:$SLOSHKarma_HOME/lib/$i
done
for j in $(ls build/jar |grep jar); do
	CLASSPATH=$CLASSPATH:$SLOSHKarma_HOME/build/jar/$j
done

echo $CLASSPATH

function usage
{
        echo 
        echo "#########################################"
        echo "#            SLOSHKarmaRun.sh             #"
        echo "#########################################"
        echo
        echo "$ SLOSHKarmaRun.sh <properties_file> <Storgae_log>"
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
java -classpath $CLASSPATH KarmaAdaptor.SLOSHRun $1 $2




