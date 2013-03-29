#!/bin/bash

# http://shared-hub1-1-sfm.ops.sfdc.net:8085/sjl/logs/2012/search/na1-indexer5-1-sjl-lu1/lu1/sfdc/na1.lu1.na1-indexer5-1-sjl.indexer.20120501.176.gmt.log.gz
echo "Starting to download I log lines..."

for server in na1-indexer5-1- na1-indexer6-1-
do
 for ts in 20120504 20120505 20120506
 do
  #export server="na1-search1-1-"
  export url="http://shared-hub1-1-sfm.ops.sfdc.net:8085/sjl/logs/2012/search/${server}sjl-lu1/lu1/sfdc/"
  export head="na1.lu1.${server}"
  export base2="sjl.indexer."
  #export ts="20111016"
  export tail=".176.gmt.log.gz"
  export filename=${url}${head}${base2}${ts}${tail}
  echo ${filename}

  if [ ! -d "$DIRECTORY" ]; then
    mkdir -p /home/asakhuja/Documents/201205_indexer_logs/${ts}
  fi

  wget -O /home/asakhuja/Documents/201205_indexer_logs/${ts}/$(basename $filename) ${filename}

 done
done

echo "Finished downloading."

