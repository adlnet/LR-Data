import json
from pprint import pformat
import urllib2
import nltk
import urllib
from urlparse import urlparse, urlunparse
from redis import StrictRedis
import couchdb
from urllib2 import urlopen
from logging import getLogger, basicConfig
from hashlib import md5
import pdb
import json
import redis.exceptions
import csv
lrUrl = "https://node01.public.learningregistry.net/harvest/listrecords"
RESUMPTION_TOKEN = "resumption_token"
FORMAT = '%(message)s'
basicConfig(format=FORMAT)
log = getLogger(__name__)
docs = {}
def load(file_name='mapping.csv'):
    result = {}
    with open(file_name, 'r') as f:
        r = csv.reader(f)
        for row in r:
            if len(row) <= 1:
                pass
            url = row[0]
            for i in row[1:]:
                result[i] = url[url.rindex('/')+1:].lower()
    return result
def handle_alignmentt(item):
    props = item.get('properties', {})
    for name in props.get("targetName"):
        yield name    
def get_standards(props, mapping):
    keys = []
    for i in props:
        for name in handle_alignmentt(i):
            keys.append(mapping.get(name, name))    
    return keys
def getItemFromList(lst):
	if lst:
		return lst.pop()
	return ""
def harvest(params, mapping):
	global lrUrl
	urlParts = urlparse(lrUrl)
	newQuery = urllib.urlencode(params)
	lrUrl = urlunparse((urlParts[0],
	                    urlParts[1],
	                    urlParts[2],
	                    urlParts[3],
	                    newQuery,
	                    urlParts[5]))
	data = {}
	resp = urlopen(lrUrl)
	try:
		data = json.load(resp)
		envelopes = (item['record']['resource_data'] for item in data['listrecords'] if "LRMI" in item['record']['resource_data']["payload_schema"])
		for envelope in envelopes:
			m = md5()
			m.update(envelope['resource_locator'])
			doc_id = m.hexdigest()
			d = None
			if "properties" not in envelope['resource_data']:				
				continue
			if doc_id not in docs:
				docs[doc_id] = {
					"_id": doc_id,
					"title": getItemFromList(envelope['resource_data']['properties'].get("alternativeHeadline", [])),
					"description": getItemFromList(envelope['resource_data']['properties'].get("description", [])),
					"publisher": envelope['identity']['owner'],
					"url": envelope['resource_locator']
					"keywords": {}
				}				
			d = docs[doc_id]
			def add_keys(keys):
				for k in (k.lower() for k in keys):
					d['keywords'][k] = d['keywords'].get(k,0) + 1				
			add_keys(envelope.get("keys", []))
			add_keys(nltk.word_tokenize(d.get("title","")))
			add_keys(nltk.word_tokenize(d.get("description","")))
			add_keys(nltk.word_tokenize(d.get("publisher","")))
			add_keys(get_standards(envelope.get('resource_data', {})\
										   .get('properties', {})\
				                           .get("educationalAlignment", []), mapping))

	except ValueError as e:
		log.exception(e)
	if RESUMPTION_TOKEN in data:
		return data[RESUMPTION_TOKEN]



def copy_values():
	source = StrictRedis()
	dest = StrictRedis(db=1)
	for key in source.keys():
		try:
			for doc_id, value in source.zrevrange(key, 0, -1, "WITHSCORES"):
				print(dest.zadd(key, value, doc_id))
				source.zrem(key, doc_id)
		except redis.exceptions.ResponseError:
			pass
	print key


if __name__ == "__main__":
	mapping = load()
	params = {"from": "2013-06-14T15:00:00Z"}
	while params is not None:
		rt = harvest(params, mapping)
		params = None
		if rt is not None and rt != "null":
			params = {RESUMPTION_TOKEN: rt}
	r = StrictRedis(db=1)
	db = couchdb.Database("http://localhost:5984/lr-data")
	for doc in docs.itervalues():
		keys = doc['keywords']
		del doc['keywords']
		try:
			del db[doc['_id']]
			db.save(doc)
			print(keys)
			for k, v in keys.iteritems():
				mult = 1
				if k in doc['title'].lower():
					mult = 10
				elif k in doc['description'].lower():
					mult = 6
				r.zadd(k, v * mult, doc['_id'])
		except:
			pass		