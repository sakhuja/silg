#!/bin/bash -x

#export SILG_MASSAGED_LOGS="Add.all_Ilines.log"

usage ()
{
   echo "\n"
   echo "Usage :"
   echo "sh main.sh <SILG_MASSAGED_LOGS> <SILG_FILTER_ORGS> <SILG_FILTERED_LOGS> <DWNLD_AND_MASSAGE>"
   echo "\nExample : \n \t sh main.sh Add.all_Ilines.log 20120501_cr_filtered_1000orgs.log top_1000_orgs_only.csv false"
   echo "\n"
}

if [ $# != 4 ]
then
    usage 
    exit
fi

export SILG_MASSAGED_LOGS=$1
export SILG_FILTER_ORGS=$2
export SILG_FILTERED_LOGS=$3
export DWNLD_AND_MASSAGE=$4

export SILG_HOME="/home/asakhuja/sandbox/aditya/IncrIndexingSearch"
export INDEXER_LOGS_UNZIPPED_ILINES="/home/asakhuja/Documents/indexer_unzipped_ilines/Ilines.log"
export MASSAGED_LOGS="/home/asakhuja/sandbox/aditya/IncrIndexingSearch/data/input/$SILG_MASSAGED_LOGS"

if [ "$DWNLD_AND_MASSAGE" == "true" ]; then
 echo "Downloading indexer logs..."
 bash I_line_downloader.sh
 echo "Finished downloading indexer logs..."

 echo "unzipping and massaging logs..."
 bash logs_massager.sh $INDEXER_LOGS_UNZIPPED_ILINES $MASSAGED_LOGS
 echo "Finished log unzipping and projecting I lines.. "
fi

## uncomment this block if you want to use different set of orgs
cd $SILG_HOME/src/main/ruby
echo "Filtering I logs..."
jruby orgs_filter.rb $SILG_MASSAGED_LOGS $SILG_FILTERED_LOGS $SILG_FILTER_ORGS
echo "finished filtering I logs as per orgs list ..."
echo "\n\n"
#exit 

echo "Generating Incr Indxing workload ..."
cd $SILG_HOME
ant clean init compile compress 
ant execute

echo "Done."
