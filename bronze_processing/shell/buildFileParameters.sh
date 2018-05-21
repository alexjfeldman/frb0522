#!/bin/bash

absolutePath=$1
folderPath=$(dirname ${absolutePath})
fullFileName=$(basename ${absolutePath}) 
fileType=${fullFileName##*.}
fileName=${fullFileName%.*}

if [[ "${fileType}" == "pdf" || "${fileType}" == "png" || "${fileType}" == "img"  ]]
then sh /home/hadoop/bronze_processing/shell/pdf_run.sh ${absolutePath}
else sh /home/hadoop/bronze_processing/shell/run.sh ${fileType} ${absolutePath} ${fileName}
fi
