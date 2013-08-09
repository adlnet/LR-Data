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
namespaces = {
    "lom": "http://ltsc.ieee.org/xsd/LOM"
}
base_xpath = "//lom:lom/lom:general/lom:{0}/lom:string[@language='en-us' or @language='en-gb' or @language='en']"
dc_namespaces = {"nsdl_dc": "http://ns.nsdl.org/nsdl_dc_v1.02/",
                 "dc": "http://purl.org/dc/elements/1.1/",
                 "dct": "http://purl.org/dc/terms/"}


def nsdl_keyword(data, config):
    s = StringIO(data['resource_data'])
    try:
        tree = etree.parse(s)
        keys = []
        result = tree.xpath('/nsdl_dc:nsdl_dc/dc:subject',
                            namespaces=dc_namespaces)
        keys.extend([subject.text for subject in result])
        result = tree.xpath('/nsdl_dc:nsdl_dc/dc:publisher',
                            namespaces=dc_namespaces)
        keys.extend([subject.text for subject in result])
        return keys
    except etree.XMLSyntaxError:
        print(data['doc_ID'])
        print(data['resource_data'])
        return []

def parse_envelope_keywords(data, config):
    keys = []
    keys.extend(data.get('keys', []))
    url_parts = urlparse.urlparse(data['resource_locator'])
    keys.append(url_parts.netloc)
    if isinstance(data['identity'], list):
        print(data['doc_ID'])
        data['identity'] = data['identity'].pop()
    keys.append(data['identity'].get("signer", ""))
    keys.append(data['identity'].get("submitter", ""))
    return keys


def handle_common_core(data, config):
    s = StringIO(data['resource_data'])
    try:
        tree = etree.parse(s)
        r = redis.StrictRedis(host=config['redis']['host'],
                              port=config['redis']['port'],
                              db=config['redis']['db'])
        query = "/nsdl_dc:nsdl_dc/dct:conformsTo"
        result = tree.xpath(query, namespaces=dc_namespaces)
        # keywords = []
        # for standard in result:
        #     s = standard.text
        #     s = s[s.rfind("/") + 1:].lower()
        #     r.incr(s+"-count")
        #     keywords.append(s)
        return [s.text for s in result]
    except:
        print(data['doc_ID'])
        print(data['resource_data'])
        return []

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
    return keywords


def parse_html(data, config):
    url = data['resource_locator']
    resp = requests.get(url)
    if "html" not in resp.headers['content-type']:
        return [], url, config
    tokens = []
    if resp.text is not None:
        raw = nltk.clean_html(resp.text)
        tokens = [t.lower() for t in nltk.word_tokenize(raw)]
    return tokens
    
def parse_lom(data, config):
    try:
        dom = etree.fromstring(data['resource_data'])
        keys = []
        found_titles = dom.xpath(base_xpath.format('title'),
                                 namespaces=namespaces)
        keys.extend([i.text for i in found_titles])
        found_description = dom.xpath(base_xpath.format('description'),
                                      namespaces=namespaces)
        keys.extend([i.text for i in found_description])
        found_keyword = dom.xpath(base_xpath.format('keyword'),
                                  namespaces=namespaces)
        keys.extend([i.text for i in found_keyword])
        return keys
    except:
        print(data['resource_data'])
        return []
