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
dc_namespaces = {"nsdl_dc": "http://ns.nsdl.org/nsdl_dc_v1.02/",
                 "dc": "http://purl.org/dc/elements/1.1/",
                 "dct": "http://purl.org/dc/terms/"}


@task(queue="parse")
def parse_envelope_keywords(data, config):
    keys = []
    keys.extend(data.get('keys', []))
    url_parts = urlparse.urlparse(data['resource_locator'])
    keys.append(url_parts.netloc)
    keys.append(data['identity'].get("signer", ""))
    keys.append(data['identity'].get("submitter", ""))
    return keys, data['resource_locator'], config


@task(queue="parse")
def parse_nsdl_dc(data, config):
    try:
        return data['resource_locator'], data['resource_data'], config
    except etree.XMLSyntaxError:
        print(data['resource_data'])

@task(queue="parse")
def handle_common_core(args):
    url, raw_tree, config = args
    s = StringIO(raw_tree)
    tree = etree.parse(s)
    r = redis.StrictRedis(host=config['redis']['host'],
                          port=config['redis']['port'],
                          db=config['redis']['db'])
    query = "/nsdl_dc:nsdl_dc/dct:conformsTo"
    result = tree.xpath(query, namespaces=dc_namespaces)
    keywords = []
    for standard in result:
        s = standard.text
        s = s[s.rfind("/") + 1:].lower()
        r.incr(s+"-count")
        keywords.append(s)
    return keywords, url, config


@task(queue="parse")
def parse_lrmi_keywords(data, config):
    metadata = data['resource_data']['items']
    keywords = []
    for item in metadata:
        keywords.extend(item.get('mediaType'))
        properties = item['properties']
        keywords.extend(properties.get('about'))
        keywords.extend(properties.get('name'))
        keywords.extend(properties.get('description'))
        keywords.extend(properties.get('learningResourceType'))
        keywords.append([x["name"] for x in properties.get("publisher")])
    return keywords, data['resource_locator'], config


@task(queue="parse")
def parse_html(data, config):
    url = data['resource_locator']
    resp = requests.get(url)
    if "html" not in resp.headers['content-type']:
        return [], url, config
    tokens = []
    if resp.text is not None:
        raw = nltk.clean_html(resp.text)
        tokens = [t.lower() for t in nltk.word_tokenize(raw)]
    return tokens, url, config    