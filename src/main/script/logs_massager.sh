#!/bin/bash

# gunzip indexer logs
# filter I line logs
# massage I line logs

set -x
export INDEXER_LOGS_ZIPPED="/home/asakhuja/Documents/201205_indexer_logs/*/*"
export INDEXER_LOGS_UNZIPPED_ILINES=$1
export MASSAGED_LOGS=$2
#exit
#if [ ! -f $OUT_LOGS ];
#then
#  echo "gunzipping indexer logs and filtering I lines ..."
#  for indexer_log in $INDEXER_LOGS_ZIPPED
#  do
   gunzip -c $INDEXER_LOGS_ZIPPED | grep '^I' >> $INDEXER_LOGS_UNZIPPED_ILINES
#  done
#fi

#echo "projecting 'I' log lines..."
#echo " result format : <timestamp>, <IndexingType>, <OrgId>, <list<prefixes>>, <records touched>"
cat $INDEXER_LOGS_UNZIPPED_ILINES | sed 's/`/#/g' | grep "#Add" | awk -F# '{ print $2","$18","$12","$26","$20 }' > $MASSAGED_LOGS

echo "Done."
set +x
