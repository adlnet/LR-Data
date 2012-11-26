import couchdb
import pyes
from pymongo import Connection
from celery.task import task
from celery.log import get_default_logger
import redis
import requests
from neo4jrestclient.client import GraphDatabase
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from BeautifulSoup import BeautifulSoup
from urlparse import urlparse
log = get_default_logger()


@task
def insertDocumentMongo(envelope, config):
    try:
        conf = config['mongodb']
        con = Connection(conf['host'], conf['port'])
        db = con[conf['database']]
        collection = db[conf['collection']]
        del envelope['_rev']
        del envelope['_id']
        collection.insert(envelope)
    except (Exception) as exc:
        log.error(exc)
        log.error("Error writing to mongo")


@task
def insertDocumentCouchdb(envelope, config):
    try:
        conf = config['couchdb']
        db = couchdb.Database(conf['dbUrl'])
        del envelope['_rev']
        del envelope['_id']
        db.save(envelope)
    except (Exception), exc:
        log.error(exc)
        log.error("Error writing to mongo")


@task
def insertDocumentElasticSearch(envelope, config):
        r = config['redis']
        r = redis.StrictRedis(host=r['host'], port=r['port'], db=r['db'])
        count = r.incr('esid')
        conf = config['elasticsearch']
        es = pyes.ES("{0}:{1}".format(conf['host'], conf['port']))
        index = {"resource_locator": envelope['resource_locator'], 'resource_data': envelope['resource_data'], 'doc_ID': envelope['doc_ID']}
        es.index(index, conf['index'], conf['index-type'], count)


@task
def insertDocumentSolr(envelope, config):
        pass


@task
def insertLRInterface(envelope, config):
    if 'keys' in envelope:
        for k in envelope['keys']:
            saveToNeo.delay(k, config)
            title = envelope['resource_locator']
            try:
                headers = requests.head(title)
                if headers.headers['content-type'] == 'text/html':
                    fullPage = requests.get(title)
                    soup = BeautifulSoup(fullPage.content)
                    title = soup.html.head.title.string
            except Exception:
                pass  # expected for invalid URLs
            cassandra_data = dict(resource_url=envelope['resource_locator'],
                                  doc_id=envelope['doc_ID'],
                                  submitter=envelope['identity']['submitter'],
                                  keyword=k)
            cassandra_data['title'] = title
            saveToCassandra.delay(cassandra_data, config)

    else:
        print(envelope)


@task
def saveToCassandra(data, config):
    r = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'], db=config['redis']['db'])
    pool = ConnectionPool('lr', server_list=['localhost', '10.10.1.47'])
    cf = ColumnFamily(pool, 'contentobjects')
    id = r.incr('cassandraid')
    cf.insert(id, data)


@task
def saveToNeo(keyword, config):
    r = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'], db=config['redis']['db'])
    gdb = GraphDatabase("http://localhost:7474/db/data/")
    if not r.sismember('topics', keyword):
        r.sadd('topics', keyword)
        gdb.nodes.create(**{"email": keyword, "topic": True})


@task
def createRedisIndex(data, config):
    r = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'], db=config['redis']['db'])
    pipe = r.pipeline()
    parts = urlparse(data['resource_locator'])
    process_keywords(r, pipe, data)
    save_display_data(pipe, parts, data)
    pipe.execute()


def process_keywords(r, pipe, data):
    for k in (key.lower() for key in data['keys']):
        keywords = k.split(' ')
        keywords.append(k)
        for keyword_part in keywords:
            if not r.zadd(k, 1.0, data['resource_locator']):
                r.zincrby(k, data['resource_locator'], 1.0)


def save_display_data(pipe, parts, data):
    title = data['resource_locator']
    try:
        title = "{0}/...{1}".format(parts.netloc, parts.path[parts.path.rfind('/'):])
        headers = requests.head(data['resource_locator'])
        if headers.headers['content-type'] == 'text/html':
            fullPage = requests.get(data['resource_locator'])
            soup = BeautifulSoup(fullPage.content)
            title = soup.html.head.title.string
    except Exception as e:
        print(e)
    print({'resource_locator': data['resource_locator'], 'title': title})
    pipe.hmset(data['resource_locator'], {'resource_locator': data['resource_locator'], 'title': title})
