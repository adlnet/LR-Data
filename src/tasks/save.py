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

stop_words = requests.get("http://jmlr.csail.mit.edu/papers/volume5/lewis04a/a11-smart-stop-list/english.stop").text.split("\n")


def create_nsdl_dc_task_tree(data, config):
    parse_sub_task = parse_nsdl_dc.s(data, config)
    common_core_chain = (parse_sub_task | handle_common_core.s() | process_keywords.s())
    nsdl_chain = (parse_sub_task | nsdl_keyword.s() | process_keywords.s())
    nsdl_display_chain = (parse_sub_task | nsdl_display_data.s())
    return [common_core_chain, nsdl_chain, nsdl_display_chain]


@task(queue="save")
def createRedisIndex(data, config):
    key_tasks = []
    display_found = False
    envelope_chain = (parse_envelope_keywords.s(data, config) | process_keywords.s())
    key_tasks.append(envelope_chain)
    schemas = [x.lower() for x in data['payload_schema']]
    if "nsdl_dc" in schemas:
        display_found = True
        key_tasks.extend(create_nsdl_dc_task_tree(data, config))
    elif "lrmi" in schemas or "nsdl_dc" in data['resource_data']:
        display_found = True
        lrmi_chain = (parse_lrmi_keywords.s(data, config) | process_keywords.s())
        key_tasks.append(lrmi_chain)
        key_tasks.append(lrmi_display_data.s(data, config))
    html_chain = (parse_html.s(data, config) | process_keywords.s())
    key_tasks.append(html_chain)
    if not display_found:
        key_tasks.append(html_display_data.s(data, config))
    key_tasks.append(save_image.s(data, config))
    group(key_tasks)()






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
