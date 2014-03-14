#! /usr/bin/env python
import redis
import urlparse
from collections import namedtuple
IndexInfo = namedtuple("IndexInfo", ["key", "value", "identifier"])

def index_netloc(url, url_parts):
    yield IndexInfo(key=url_parts.netloc, value=1, identifier=url)

def index_path(url, url_parts):
    for segment in url_parts.path.split('/'):
        yield IndexInfo(key=segment, value=1, identifier=url)

def index_query(url, url_parts):
    for k,vs in urlparse.parse_qs(url_parts.query).iteritems():        
        yield IndexInfo(key=k, value=1, identifier=url)
        for v in vs:
            yield IndexInfo(key=v, value=1, identifier=url)
            yield IndexInfo(key="{0}={1}".format(k,v), value=1, identifier=url)

def get_keys(url, url_parts):
    generators = [index_path, index_query]
    for func in generators:
        for index_tuple in func(url, url_parts):
            yield index_tuple.key


client = redis.StrictRedis(host="localhost", port=6379, db=0)

test_url = "http://www.uciteljska.net/ucit_dl.php?id=86"

parts = urlparse.urlparse(test_url)

keys = list(get_keys(test_url, parts))

client.delete("union_result")

client.zunionstore("union_result", keys)

client.delete("final_result")

client.zinterstore("final_result", [parts.netloc, "union_result"])

results = client.zrange("final_result", 0, 10, desc=True, withscores=True)

for r in results:
    print(r)