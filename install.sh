#!/bin/bash

STANDARDSDB=http://localhost:5984/standards
LRDATADB=http://localhost:5984/lr-data
FLAGGEDDB=http://localhost:5984/flagged
ELASTICSEARCH=elasticsearch-0.90.10.deb
ELASTICSEARCHDOWNLOAD=https://download.elasticsearch.org/elasticsearch/elasticsearch/
sudo apt-get install -y rabbitmq-server redis-server python-virtualenv 
sudo apt-get install -y libxml2-dev libxslt1-dev couchdb xvfb
if [ ! -e $ELASTICSEARCH ]; then
    echo "Downloading Elasticsearch"
    wget $ELASTICSEARCHDOWNLOAD$ELASTICSEARCH
fi
echo "Installing Elasticsearch"
sudo dpkg -i $ELASTICSEARCH
rm $ELASTICSEARCH
virtualenv env
. env/bin/activate
pip install -r requirements.txt
declare -a DBS=($STANDARDSDB $LRDATADB $FLAGGEDDB);
echo "Creating CouchDB Databases"
for i in "${DBS[@]}"
do
    echo "CREATING " $i
    curl -XPUT $i
done
echo "Harvesting Common Core Standards Info"
pushd cc
python process.py
popd
pushd couchdb/flagged
couchapp push $FLAGGEDDB
popd
pushd couchdb/gov
couchapp push $LRDATADB
popd
pushd couchdb/publisher_view
couchapp push $LRDATADB
popd
pushd couchdb/standards
couchapp push $STANDARDSDB
popd
