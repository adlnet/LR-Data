import couchdb
import pyes
from pymongo import Connection
from celery.task import task
from celery.log import get_default_logger
import redis
from neo4jrestclient.client import GraphDatabase
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
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
    r = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'], db=config['redis']['db'])
    pool = ConnectionPool('lr')
    cf = ColumnFamily(pool, 'contentobjects')
    gdb = GraphDatabase("http://localhost:7474/db/data/")
    if 'keys' in envelope:
        for k in envelope['keys']:
            if not r.sismember('topics', k):
                r.sadd('topics', k)
                gdb.nodes.create(**{"emaiL": k, "topic": True})
            id = r.incr('cassandraid')
            print(envelope)
            cf.insert(id, dict(resource_url=envelope['resource_locator'], doc_id=envelope['doc_ID'], submitter=envelope['identity']['submitter'], keyword=k))
    else:
        print(envelope)
