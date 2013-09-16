import couchdb
from celery.task import task
from celery.log import get_default_logger
import redis
import nltk
from nltk.stem.snowball import PorterStemmer
import requests
import urllib2
import hashlib
import urlparse
from lxml import etree
from StringIO import StringIO
import subprocess
from BeautifulSoup import BeautifulSoup
import os
from celery import group, chain, chord
from .display import *
import pyes
import csv
log = get_default_logger()
conn = pyes.ES([("http", "localhost", "9200")])
INDEX_NAME = "lr"
DOC_TYPE = "lr_doc"

def index(doc, doc_id):
    update_function = 'ctx._source.keys.addAll(keys);ctx._source.standards.addAll(standards);'
    print(conn.partial_update(INDEX_NAME, DOC_TYPE, doc_id, update_function, upsert=doc, params=doc))

def get_html_display(url, publisher):
    md5 = hashlib.md5()
    md5.update(url)   
    doc_id = md5.hexdigest()
    try: 
        resp = urllib2.urlopen(url)
        if not resp.headers['content-type'].startswith('text/html'):
            return
        raw = resp.read()
        raw = raw.decode('utf-8')
        soup = BeautifulSoup(raw)
        title = url
        if soup.html is not None and \
                soup.html.head is not None and \
                soup.html.head.titl is not None:
            title = soup.html.head.title.string            
        raw = nltk.clean_html(raw)
        tokens = nltk.word_tokenize(raw)
        description = " ".join(tokens[:100])
        return {
            "title": title,
            "description": description,
            "url": url,
            "publisher": publisher,
            "hasScreenshot": False
            }
    except Exception as ex:
        print(ex)
        return {
            "title": url,
            "description": url,
            "publisher": publisher,
            "url" :url,
            "hasScreenshot": False
            }

def process_nsdl_dc(envelope, mapping):
    md5 = hashlib.md5()
    md5.update(envelope['resource_locator'])          
    doc_id = md5.hexdigest()
    #parse the resource_data into an XML dom object
    dom = etree.fromstring(envelope['resource_data'])
    #dictionary containing XML namespaces and their prefixes
    dc_namespaces = {"nsdl_dc": "http://ns.nsdl.org/nsdl_dc_v1.02/",
                     "dc": "http://purl.org/dc/elements/1.1/",
                     "dct": "http://purl.org/dc/terms/"}
    # run an XPath query againt the dom object that pulls out all the document titles
    standards = dom.xpath('/nsdl_dc:nsdl_dc/dct:conformsTo',
                       namespaces=dc_namespaces)
    # extract a set of all the titles from the DOM elements
    standards = {elm.text[elm.text.rfind('/') + 1:].lower() for elm in standards}
    standards = (mapping.get(s, [s]) for s in standards)
    keys = envelope['keys']
    final_standards = []
    for ids in standards:
        for s in ids:
            final_standards.append(s)
            #client.zrem(s, envelope['doc_ID'])
            #client.zadd(s, 1, doc_id)
    try:
        md5 = hashlib.md5()
        title = dom.xpath('/nsdl_dc:nsdl_dc/dc:title', namespaces=dc_namespaces)
        if title:
            title = title.pop().text
        else:
            title = envelope['resource_locator']
        description = dom.xpath('/nsdl_dc:nsdl_dc/dc:description', namespaces=dc_namespaces)
        if description:
            description = description.pop().text
        else:
            description = envelope['resource_locator']
        doc = {
        "title": title,
        'publisher': envelope['identity']['submitter'],
        'hasScreenshot': False,
	"description": description,
	"url": envelope['resource_locator'],
        "keys": keys,
        "standards": final_standards
        }
        index(doc, doc_id)
    except Exception as ex:
        print(ex)


def process_lrmi(envelope, mapping):
    #LRMI is json so no DOM stuff is needed
    resource_data = envelope.get('resource_data', {})
    properties = resource_data.get('properties', {})
    educational_alignment = properties.get('educationalAlignment', [{}]).pop()
    educational_alignment_properties = educational_alignment.get('properties', {})
    standards_names = educational_alignment_properties.get('targetName', [''])
    md5 = hashlib.md5()
    md5.update(envelope['resource_locator'])
    doc_id = md5.hexdigest()
    keys = envelope['keys']
    standards = []
    for ids in (mapping.get(standards_name.lower(), [standards_name.lower()]) for standards_name in standards_names):
        for s in ids:
            standards.append(s)
            #client.zrem(s, envelope['doc_ID'])
            #client.zadd(s, 1, doc_id)
    doc = {
        "url": envelope['resource_locator'],
        "keys": keys,
        "standards": standards,
        "title": resource_data.get('name', '').decode('utf8'),
        "description": resource_data.get('description', '').decode('utf8'),
        'hasScreenshot': False,
        'publisher': envelope['identity']['submitter']
    }
    try:
        index(doc, doc_id)
    except Exception as ex:
        print(ex)


def process_lr_para(envelope, mapping):
    md5 = hashlib.md5()
    md5.update(envelope['resource_locator'])
    doc_id = md5.hexdigest()
    activity = envelope.get('resource_data', {}).get('activity', {})
    action = activity.get('verb', {}).get('action')
    keys = envelope['keys']
    standards = []
    if "matched" == action:
        for a in activity.get('related', []):
            standards = a.get('id', '').lower()
            standard_id = standard[standard.rfind('/') + 1:]
            for s in mapping.get(standard_id, [standard_id]):
                standards.append(s)
    try:
        doc = get_html_display(envelope['resource_locator'], envelope['identity']['submitter'])
        doc['keys'] = keys
        doc['standards'] = standards
        index(doc, doc_id)
    except Exception as ex:
        print(ex)

def process_lom(data, config):
    try:
        md5 = hashlib.md5()
        md5.update(data['resource_locator'])
        doc_id = md5.hexdigest()
        base_xpath = "//lom:lom/lom:general/lom:{0}/lom:string[@language='en-us' or @language='en-gb' or @language='en']"
        dc_namespaces = {"nsdl_dc": "http://ns.nsdl.org/nsdl_dc_v1.02/",
                         "dc": "http://purl.org/dc/elements/1.1/",
                         "dct": "http://purl.org/dc/terms/"}
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
        title = data['resource_locator']
        if not found_titles:
            title = found_titles.pop().text
        desc = data['resource_locator']
        if not found_description:
            desc = found_description.pop().text
        doc = {
            "url": data['resource_locator'],
            "keys": keys,
            "standards": [],
            "title": title,
            "description": desc,
            'hasScreenshot': False,
            'publisher': data['identity']['submitter']
            }
        index(doc, doc_id)
    except Exception as ex:
        print(ex)

def process_generic(envelope):
    md5 = hashlib.md5()
    md5.update(envelope['resource_locator'])
    doc_id = md5.hexdigest()
    keys = envelope['keys']
    standards = []
    try:
        doc = get_html_display(envelope['resource_locator'], envelope['identity']['submitter'])
        doc['keys'] = keys
        doc['standards'] = standards
        index(doc, doc_id)
    except Exception as ex:
        print(ex)

def load_standards(file_name):
    mapping = {}
    with open(file_name, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            for item in row:
                if 'asn.jesandco.org' in item:
                    continue
                for item2 in row:
                    if item != item2:
                        if 'asn.jesandco.org' in item2:
                            item2 = item2[item2.rfind('/')+1:]
                        else:
                            continue
                        key = item.lower()
                        if key not in mapping:
                            mapping[key] = []
                        mapping[key].append(item2.lower())
    return mapping
        
@task(queue="save")
def createRedisIndex(envelope, config):
    log.debug("Begin Creating Index")
    #normalize casing on all the schemas in the payload_schema array, if payload_schema isn't present use an empty array
    schemas = {schema.lower() for schema in envelope.get('payload_schema', [])}
    mapping = load_standards("mapping.csv")
    try:
        if "lr paradata 1.0" in schemas:
            process_lr_para(envelope, mapping)
        elif 'nsdl_dc' in schemas:
            process_nsdl_dc(envelope, mapping)
        elif 'lrmi' in schemas:
            process_lrmi(envelope, mapping)
        elif "lom" in schemas:
            process_lom(envelope, mapping)
        else:
            process_generic(envelope)
    except Exception as ex:
        print(ex)
    save_image.delay(envelope, config)

@task(queue="image")
def save_image(envelope, config):
    m = hashlib.md5()
    m.update(envelope['resource_locator'])
    couchdb_id = m.hexdigest()
    p = subprocess.Popen(" ".join(["xvfb-run", "--auto-servernum", "--server-num=1", "python", "screenshots.py", envelope['resource_locator'], couchdb_id]), shell=True, cwd=os.getcwd(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    filename = p.communicate()
    print(filename)
