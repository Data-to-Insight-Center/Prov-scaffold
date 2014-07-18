#!/bin/sh
JAVA_HOME=
Prov-scaffold_HOME=/home/quzhou/SLOSHKarma
Provenance_Repo_HOME=/home/quzhou/KarmaClient

cd $Provenance_Repo_HOME
for i in $(ls lib |grep jar); do
	CLASSPATH=$CLASSPATH:$Provenance_Repo_HOME/lib/$i
done
for j in $(ls build/lib |grep jar); do
	CLASSPATH=$CLASSPATH:$Provenance_Repo_HOME/build/lib/$j
done

cd $SProv-scaffold_HOME

for i in $(ls lib |grep jar); do
	CLASSPATH=$CLASSPATH:$Prov-scaffold_HOME/lib/$i
done
for j in $(ls build/jar |grep jar); do
	CLASSPATH=$CLASSPATH:$Prov-scaffold_HOME/build/jar/$j
done

echo $CLASSPATH

function usage
{
        echo 
        echo "#########################################"
        echo "#            Prov-scaffold-Run.sh             #"
        echo "#########################################"
        echo
        echo "$ Prov-scaffold-Run.sh <provenance_repo_properties_file> <System_log>"
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




