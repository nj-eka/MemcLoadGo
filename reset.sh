#!/bin/bash
lsof -nti:33013,33014,33015,33016 | xargs -r kill -9
memcached -l 0.0.0.0:33013,0.0.0.0:33014,0.0.0.0:33015,0.0.0.0:33016 &
lsof -i | grep memcached
for f in data/appsinstalled/.*.tsv.gz
do  
    echo "$f > data/appsinstalled/${f#*.}"
    mv "$f" "data/appsinstalled/${f#*.}"
done
for f in data/appsinstalled/_*.tsv.gz
do
    echo "rm -rf $f"
    rm -rf "$f"
done
for f in *.log
do
    echo "rm -rf $f"
    rm -rf "$f"
done
echo "rm -rf data/dques"
rm -rf data/dques
