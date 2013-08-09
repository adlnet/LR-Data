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
from .display import *
from .index import *
from .parse import *
log = get_default_logger()
dc_namespaces = {"nsdl_dc": "http://ns.nsdl.org/nsdl_dc_v1.02/",
                 "dc": "http://purl.org/dc/elements/1.1/",
                 "dct": "http://purl.org/dc/terms/"}


@task(queue="save")
def createRedisIndex(data, config):
    key_tasks = []
    display_found = False
    schemas = [x.lower() for x in data['payload_schema']]
    keys = parse_envelope_keywords(data, config)
    if "nsdl_dc" in schemas or "nsdl_dc" in data['resource_data']:
        display_found = True        
        nsdl_display_data.delay(data, config)
        keys.extend(handle_common_core(data, config))
        keys.extend(nsdl_keyword(data, config))
    elif "lrmi" in schemas:
        display_found = True
        lrmi_display_data.delay(data, config)
        keys.extend(parse_lrmi_keywords(data, config))
    elif "lom" in schemas:
        display_found = True
        lom_display.delay(data, config)
        keys.extend(parse_lom(data, config))
    process_keywords.delay(keys, data['resource_locator'], data['doc_ID'], config)
    process_keywords.delay(parse_html(data, config), data['resource_locator'], 'web', config)
    if not display_found:
        html_display_data.delay(data, config)
    save_image.delay(data, config)

@task(queue="image")
def save_image(envelope, config):
    m = hashlib.md5()
    m.update(envelope['resource_locator'])
    couchdb_id = m.hexdigest()
    p = subprocess.Popen(" ".join(["xvfb-run", "--auto-servernum", "--server-num=1", "python", "screenshots.py", envelope['resource_locator'], couchdb_id]), shell=True, cwd=os.getcwd(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    filename = p.communicate()
    print(filename)
    db = couchdb.Database(config['couchdb']['dbUrl'])
    if couchdb_id not in db:
        db[couchdb_id] = {"_id": couchdb_id}
    try:
        with open(os.path.join(os.getcwd(), couchdb_id+"-thumbnail.jpg"), "rb") as f:
            db.put_attachment(db[couchdb_id], f, "thumbnail.jpeg", "image/jpeg")
    except IOError as e:
        log.exception(e)
    try:
        with open(os.path.join(os.getcwd(), couchdb_id+"-screenshot.jpg"), "rb") as f:
            db.put_attachment(db[couchdb_id], f, "screenshot.jpeg", "image/jpeg")
    except IOError as e:
        log.exception(e)
    for file_to_delete in [couchdb_id+"-thumbnail.jpg", couchdb_id+"-screenshot.jpg", couchdb_id + ".jpg"]:
        try:
            os.remove(os.path.join(os.getcwd(), file_to_delete))
        except OSError as e:
            log.error(os.path.join(os.getcwd(), file_to_delete))
