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

                 
@task(queue="display")
def html_display_data(data, config):
    url = data['resource_locator']
    resp = requests.get(url)
    if not resp.headers['content-type'].startswith('text/html'):
        return
    raw = resp.text
    try:
        raw = raw.decode('utf-8')
    except:
        pass
    try:
        soup = BeautifulSoup(raw)
        title = url
        if soup.html is not None and soup.html.head is not None:
            title = soup.html.head.title.string
            
        raw = nltk.clean_html(resp.text)
        tokens = nltk.word_tokenize(raw)
        description = " ".join(tokens[:100])
        publisher = format_publisher(None, data)
        save_display_data(title, description, publisher, url, config)
    except Exception as ex:
        log.exception(ex)
        log.error(url)


@task(queue="display")
def lrmi_display_data(data, config):
    metadata = data['resource_data']['items'][0]['properties']
    raw_title = metadata.get("name", [""]).pop()
    if isinstance(raw_title, dict):
        title = raw_title["name"]
    elif isinstance(raw_title, str):
        title = raw_title
    else:
        title = ""
    description = metadata.get("description", [""]).pop()
    raw_publisher = metadata.get("publisher", [""]).pop()
    if isinstance(raw_publisher, dict):
        publisher = raw_publisher["name"]
    elif isinstance(raw_publisher, str):
        publisher = raw_publisher
    else:
        publisher = ""
    save_display_data(title, description, publisher, data['resource_locator'], config)


def format_publisher(publisher, data):
    if publisher is None:
        raw_identity = data['identity']
        if isinstance(raw_identity, list):
            print(raw_identity)
            raw_identity = raw_identity.pop()
        curator = raw_identity.get("curator", None)
        owner = raw_identity.get("owner", None)

        if curator is not None and owner is not None and curator.strip() != owner.strip():
            publisher = "{0}, supported by {1}".format(curator, owner)
        elif curator is not None:
            publisher = curator
        else:
            signer = data['identity'].get("signer", "")
            submitter = data['identity'].get("submitter", "")
            if len(signer) > len(submitter):
                publisher = signer
            elif len(submitter) > len(signer):
                publisher = submitter
            else:
                publisher = ""
    return publisher
    
def save_display_data(title, description, publisher, resource_locator, config):
    m = hashlib.md5()
    m.update(resource_locator)
    couchdb_id = m.hexdigest()
    conf = config['couchdb']
    db = couchdb.Database(conf['dbUrl'])
    doc = {"_id": couchdb_id}
    try:
        doc["title"] = title.strip()
        doc["description"] = description.strip()
        doc["url"] = resource_locator.strip()
        doc['publisher'] = publisher.strip()
        for k, v in doc.iteritems():
            try:
                doc[k] = v.decode('utf-8')
            except:
                pass
        if couchdb_id in db:
            doc = db[couchdb_id]
        db.save(doc)
    except Exception as ex:
        log.exception(ex)
        log.error(description)
        log.error(title)
        log.error(resource_locator)
        log.error(publisher)

@task(queue="display")
def nsdl_display_data(data, config):
    s = StringIO(data['resource_data'])
    try:
        tree = etree.parse(s)
        result = tree.xpath('/nsdl_dc:nsdl_dc/dc:title',
                            namespaces=dc_namespaces)
        title = data['resource_locator']
        if len(result) > 0:
            title = result[0].text
        result = tree.xpath('/nsdl_dc:nsdl_dc/dc:description',
                            namespaces=dc_namespaces)
        description = data['resource_locator']
        if len(result) > 0:
            description = result[0].text
        result = tree.xpath('/nsdl_dc:nsdl_dc/dc:publisher',
                            namespaces=dc_namespaces)
        publisher = data['identity'].get('curator', '')
        if len(result) > 0:
            publisher = result[0].text
        save_display_data(title, description, publisher, data['resource_locator'], config)
    except etree.XMLSyntaxError as ex:
        log.exception(ex)


@task(queue="display")
def lom_display(data, config):
    try:
        dom = etree.fromstring(data['resource_data'])
        found_titles = dom.xpath(base_xpath.format('title'),
                                 namespaces=namespaces)
        title = data['resource_locator']
        if len(found_titles) > 0:            
            title = found_titles.pop().text
        found_description = dom.xpath(base_xpath.format('description'),
                                      namespaces=namespaces)
        description = data['resource_locator']
        if len(found_description) > 0:            
            description = found_description.pop().text
        save_display_data(title, description, data.get('identity', {}).get('curator',''), data['resource_locator'], config)
    except Exception as ex:
        log.error(data['resource_data'])
        log.exception(ex)
    
