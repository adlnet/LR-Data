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
    elif "lrmi" in schemas:
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


def save_to_index(k, value, r):
    keywords = k.split(' ')
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


@task(queue="display")
def nsdl_display_data(args):
    resource_locator, raw_tree, config = args
    s = StringIO(raw_tree)
    tree = etree.parse(s)
    result = tree.xpath('/nsdl_dc:nsdl_dc/dc:title',
                        namespaces=dc_namespaces)
    title = result[0].text
    result = tree.xpath('/nsdl_dc:nsdl_dc/dc:description',
                        namespaces=dc_namespaces)
    description = result[0].text
    result = tree.xpath('/nsdl_dc:nsdl_dc/dc:publisher',
                        namespaces=dc_namespaces)
    publisher = result[0].text
    print(title)
    print(description)
    save_display_data(title, description, publisher, resource_locator, config)


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


@task(queue="display")
def lrmi_display_data(data, config):
    metadata = data['resource_data']['items'][0]['properties']
    title = metadata.get("name", [""]).pop()
    description = metadata.get("description", [""]).pop()
    raw_publisher = metadata.get("publisher", [""]).pop()
    if isinstance(raw_publisher, dict):
        publisher = raw_publisher["name"]
    elif isinstance(raw_publisher, str):
        publisher = raw_publisher
    else:
        publisher = ""
    save_display_data(title, description, publisher, data['resource_locator'], config)


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


def format_publisher(publisher, data):
    if publisher is None:
        curator = data['identity'].get("curator", None)
        owner = data['identity'].get("owner", None)
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


@task(queue="display")
def html_display_data(data, config):
    url = data['resource_locator']
    resp = requests.get(url)
    soup = BeautifulSoup(resp.content)
    if soup.html is not None:
        title = soup.html.head.title.string
    else:
        title = url
    raw = nltk.clean_html(resp.text)
    tokens = nltk.word_tokenize(raw)
    description = " ".join(tokens[:100])
    publisher = format_publisher(None, data)
    save_display_data(title, description, publisher, url, config)


def save_display_data(title, description, publisher, resource_locator, config):
    m = hashlib.md5()
    m.update(resource_locator)
    couchdb_id = m.hexdigest()
    conf = config['couchdb']
    db = couchdb.Database(conf['dbUrl'])
    try:
        doc = {"_id": couchdb_id}
        if couchdb_id in db:
            doc = db[couchdb_id]
        doc["title"] = title.strip()
        doc["description"] = description.strip()
        doc["url"] = resource_locator.strip()
        doc['publisher'] = publisher.strip()
        db[couchdb_id] = doc
    except Exception as ex:
        print(ex)


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
    print(couchdb_id)
