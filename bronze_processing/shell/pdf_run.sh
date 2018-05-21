#!/bin/bash

absolutePath=$1

fileLand="/home/hadoop/tmp/land/"$(basename ${absolutePath})
fileOutput="/home/hadoop/tmp/output/"$(basename ${absolutePath})

if [[ ! -d /home/hadoop/tmp/land ]]
then mkdir -p /home/hadoop/tmp/land
fi

if [[ ! -d /home/hadoop/tmp/output ]]
then mkdir -p /home/hadoop/tmp/output
fi

if [[ -f ${fileLand} ]]
then rm ${fileLand}
fi

if [[ -f ${fileOutput} ]]
then rm ${fileOutput}
fi

aws s3 cp ${absolutePath} ${fileLand}

java -jar /home/hadoop/Tika-Config-Files/tika-app-1.8.jar -t ${fileLand} > ${fileOutput}

echo $(basename ${absolutePath})" has been succedfully parsed."
