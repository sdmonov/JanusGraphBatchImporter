#!/bin/sh

JANUSGRAPH_PATH=../../janusgraph

CP="lib/commons-csv-1.4.jar:conf:$JANUSGRAPH_PATH/lib/*"

java -cp $CP:lib/batchimport.jar com.ibm.janusgraph.utils.importer.BatchImport $@
