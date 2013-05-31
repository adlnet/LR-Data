import couchdb
from celery.task import task
from celery.log import get_default_logger
import redis
import nltk
from nltk.stem.snowball import PorterStemmer
import requests
import hashlib
import urlparse
from lxml import etree
from StringIO import StringIO
import subprocess
from BeautifulSoup import BeautifulSoup
import os
from celery import group, chain, chord
log = get_default_logger()

def save_to_index(k, value, r):
    keywords = nltk.word_tokenize(k)
    keywords.append(k)
    #ascii ranges for punction marks
    #should probably use a regex for this
    punctuation = range(32, 48)
    punctuation.extend(range(58, 65))
    punctuation.extend(range(91, 97))
    punctuation.extend(range(123, 128))
    for keyword_part in keywords:
        print(keyword_part)
        if keyword_part in stop_words:
            continue  # don't index stop words
        if len(keyword_part) == 1:
            continue  # don't index single characters
        if reduce(lambda x, y: x and (ord(y) in punctuation), keyword_part, True):
            continue  # don't index if the entire string is punctuation
        if not r.zadd(keyword_part, 1.0, value):
            r.zincrby(keyword_part, value, 1.0)



@task(queue="index")
def process_keywords(args):
    keywords, resource_locator, config = args
    r = redis.StrictRedis(host=config['redis']['host'],
                          port=config['redis']['port'],
                          db=config['redis']['db'])
    m = hashlib.md5()
    m.update(resource_locator)
    url_hash = m.hexdigest()
    for k in (key.lower() for key in keywords):
        save_to_index(k, url_hash, r)


@task(queue="index")
def nsdl_keyword(args):
    resource_locator, raw_tree, config = args
    s = StringIO(raw_tree)
    tree = etree.parse(s)
    try:
        keys = []
        result = tree.xpath('/nsdl_dc:nsdl_dc/dc:subject',
                            namespaces=dc_namespaces)
        keys.extend([subject.text for subject in result])
        result = tree.xpath('/nsdl_dc:nsdl_dc/dc:publisher',
                            namespaces=dc_namespaces)
        keys.extend([subject.text for subject in result])
        return keys, resource_locator, config
    except etree.XMLSyntaxError:
        print(resource_locator)