import couchdb
from celery.task import task
from celery.log import get_default_logger
import redis
import nltk
from nltk.stem.snowball import PorterStemmer
import riak
import hashlib
import requests
import json
from lxml import etree
from StringIO import StringIO
log = get_default_logger()
stop_words = requests.get("http://jmlr.csail.mit.edu/papers/volume5/lewis04a/a11-smart-stop-list/english.stop").text.split("\n")
dc_namespaces = {"nsdl_dc": "http://ns.nsdl.org/nsdl_dc_v1.02/",
                 "dc": "http://purl.org/dc/elements/1.1/",
                 "dct": "http://purl.org/dc/terms/"}

def process_keys(keywords):
    for k in keywords:
        if isinstance(k, list):
            for sk in process_keys(k):
                yield sk
        elif isinstance(k, dict):
            print(k)
        else:
            k = k.lower()
            keywords = nltk.word_tokenize(k)
            #ascii ranges for punction marks
            #should probably use a regex for this
            punctuation = range(32, 48)
            punctuation.extend(range(58, 65))
            punctuation.extend(range(91, 97))
            punctuation.extend(range(123, 128))
            for keyword_part in keywords:
                if keyword_part in stop_words:
                    continue  # don't index stop words
                if len(keyword_part) == 1:
                    continue  # don't index single characters
                if reduce(lambda x, y: x and (ord(y) in punctuation), keyword_part, True):
                    continue  # don't index if the entire string is punctuation
                yield keyword_part



@task(queue="index")
def process_keywords(keywords, resource_locator, doc_id, config):
    r = redis.StrictRedis(host=config['redis']['host'],
                          port=config['redis']['port'],
                          db=config['redis']['db'])
    m = hashlib.md5()
    m.update(resource_locator)
    url_hash = m.hexdigest()
    for k in process_keys(keywords):
        r.sadd(k, url_hash)

#def process_keywords(keywords, resource_locator, doc_id, config):
#    client = riak.RiakClient(host=config['riak']['host'],
#                             port=config['riak']['port'],
#                             protocol=config['riak']['protocol'])    
#    m = hashlib.md5()
#    m.update(resource_locator)
#    url_hash = m.hexdigest()

#    keywords = list(process_keys(keywords))
#    bucket = client.bucket(url_hash)
#    key = bucket.new(doc_id, keywords)
#    key.store()
