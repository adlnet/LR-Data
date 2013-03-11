import couchdb
from celery.task import task
from celery.log import get_default_logger
import redis
from urlparse import urlparse
import nltk
import requests
import hashlib
from lxml import etree
from StringIO import StringIO
import subprocess
import os
from celery import group, chain
log = get_default_logger()
dc_namespaces = {"nsdl_dc": "http://ns.nsdl.org/nsdl_dc_v1.02/",
                 "dc": "http://purl.org/dc/elements/1.1/",
                 "dct": "http://purl.org/dc/terms/"}

stop_words = requests.get("http://jmlr.csail.mit.edu/papers/volume5/lewis04a/a11-smart-stop-list/english.stop").text.split("\n")


@task(queue="save")
def createRedisIndex(data, config):
    key_tasks = []
    envelope_chain = (parse_envelope_keywords.s(data, config) | process_keywords.s())
    key_tasks.append(envelope_chain)
    schemas = [x.lower() for x in data['payload_schema']]
    if "nsdl_dc" in schemas:
        common_core_chain = (handle_common_core.s() | process_keywords.s())
        parse_result_group = group(nsdl_keyword.s(), common_core_chain)
        parse_chain = (parse_nsdl_dc.s(data, config) | parse_result_group)
        save_nsdl_keys = (parse_chain | process_keywords.s())
        key_tasks.append(save_nsdl_keys)

    html_chain = (parse_html.s(data, config) | process_keywords.s())
    key_tasks.append(html_chain)
    group(key_tasks)()


def save_to_index(k, value, r):
    keywords = k.split(' ')
    keywords.append(k)
    for keyword_part in keywords:
        if keyword_part in stop_words:
            continue
        log.debug(keyword_part)
        print(keyword_part)
        if not r.zadd(keyword_part, 1.0, value):
            r.zincrby(keyword_part, value, 1.0)


@task(queue="parse")
def parse_envelope_keywords(data, config):
    return data['keys'], data['resource_locator'], config


@task(queue="parse")
def parse_nsdl_dc(data, config):
    try:
        s = StringIO(data['resource_data'])
        tree = etree.parse(s)
        return data['resource_locator'], tree, config
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
    resource_locator, tree, config = args
    try:
        result = tree.xpath('/nsdl_dc:nsdl_dc/dc:subject',
                            namespaces=dc_namespaces)
        return [subject.text for subject in result], resource_locator, config
    except etree.XMLSyntaxError:
        print(resource_locator)


@task(queue="parse")
def handle_common_core(args):
    url, tree, config = args
    r = redis.StrictRedis(host=config['redis']['host'],
                          port=config['redis']['port'],
                          db=config['redis']['db'])
    query = "/nsdl_dc:nsdldc/dct:conformsTo"
    result = tree.query(query, namespaces=dc_namespaces)
    keywords = []
    for standard in result:
        r.incr(standard.text)
        keywords.append(standard.text)
    return keywords, url, config


@task(queue="display")
def nsdl_display_data(resource_locator, tree, config):
    result = tree.xpath('/nsdl_dc:nsdl_dc/dc:title',
                        namespaces=dc_namespaces)
    title = result[0].text
    result = tree.xpath('/nsdl_dc:nsdl_dc/dc:description',
                        namespaces=dc_namespaces)
    description = result[0].text
    result = tree.xpath('/nsdl_dc:nsdl_dc/dc:publisher',
                        namespaces=dc_namespaces)
    publisher = result[0].text
    save_display_data(title, description, publisher, resource_locator, config)


@task(queue="display")
def lrmi_display_data(data, config):
    metadata = data['resource_data']['items'][0]['properties']
    title = metadata.get("name", [""]).pop()
    description = metadata.get("description", [""]).pop()
    publisher = metadata.get("publisher", [""]).pop()["name"]
    save_display_data(title, description, publisher, data['resource_locator'], config)


@task(queue="parse")
def parse_html(data, config):
    url = data['resource_locator']
    resp = requests.get(url)
    raw = nltk.clean_html(resp.text)
    tokens = [t.lower for t in nltk.word_tokenize(raw)]
    return tokens, url, config



'''
def save_display_data(parts, data, config):
    title = data['resource_locator']
    description = ""
    publisher = None

    try:
        headers = requests.head(data['resource_locator'])
        if 'nsdl_dc' in data['payload_schema']:

        elif "LRMI" in data['payload_schema']:

        elif headers.headers['content-type'].startswith('text/html'):
            fullPage = requests.get(data['resource_locator'])
            soup = BeautifulSoup(fullPage.content)
            title = soup.html.head.title.string
        else:
            title = "{0}/...{1}".format(parts.netloc,
                                        parts.path[parts.path.rfind('/'):])
        if publisher is None:
            curator = data['identity'].get("curator", None)
            owner = data['identity'].get("owner", None)
            if curator is not None and owner is not None and curator != owner:
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
    except Exception as e:
        print(e)
'''


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
        doc["title"] = title
        doc["description"] = description
        doc["url"] = resource_locator
        doc['publisher'] = publisher
        print(doc)
        db[couchdb_id] = doc
        print(doc)
        print("saved to couch")
    except Exception as ex:
        print(ex)


@task(queue="image")
def save_image(envelope, config):
    m = hashlib.md5()
    m.update(envelope['resource_locator'])
    couchdb_id = m.hexdigest()
    print('get lock')
    p = subprocess.Popen(" ".join(["xvfb-run", "--auto-servernum", "--server-num=1", "python", "screenshots.py", envelope['resource_locator'], couchdb_id]), shell=True, cwd=os.getcwd(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    filename = p.communicate()
    print(filename)
    print(couchdb_id)
    db = couchdb.Database(config['couchdb']['dbUrl'])
    try:
        with open(os.path.join(os.getcwd(), couchdb_id+"-thumbnail.jpg"), "rb") as f:
            db.put_attachment(db[couchdb_id], f, "thumbnail.jpeg", "image/jpeg")
            print("uploaded")
    except IOError as e:
        log.debug(os.path.join(os.getcwd(), couchdb_id+"-thumbnail.jpg"))
        log.exception(e)
    try:
        with open(os.path.join(os.getcwd(), couchdb_id+"-screenshot.jpg"), "rb") as f:
            db.put_attachment(db[couchdb_id], f, "screenshot.jpeg", "image/jpeg")
            print("uploaded")
    except IOError as e:
        log.exception(e)
    for file_to_delete in [couchdb_id+"-thumbnail.jpg", couchdb_id+"-screenshot.jpg", couchdb_id + ".jpg"]:
        try:
            os.remove(os.path.join(os.getcwd(), file_to_delete))
        except OSError as e:
            log.error(os.path.join(os.getcwd(), file_to_delete))
            print(e)
