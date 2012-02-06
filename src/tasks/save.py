import couchdb
import pyes
import pysolr
from pymongo import Connection
from celery.task import task
from celery.log import get_default_logger
import redis
log = get_default_logger()
@task
def insertDocumentMongo(envelope, config):
    try:
        conf = config['mongodb']
        con = Connection(conf['host'],conf['port'])
        db = con[conf['database']]
        collection = db[conf['collection']]   
        del envelope['_rev']
        del envelope['_id']
        collection.insert(envelope)
    except (Exception), exc:
        log.error("Error writing to mongo")
        processHarvestResult.retry(exc)    
@task
def insertDocumentCouchdb(envelope,config):
    try:
        conf = config['couchdb']
        db = couchdb.Database(conf['dbUrl'])
        del envelope['_rev']
        del envelope['_id']
        db.save(envelope)
    except (Exception), exc:
        log.error("Error writing to mongo")
        processHarvestResult.retry(exc)        
@task
def insertDocumentElasticSearch(envelope,config):
        r = config['redis']
        r = redis.StrictRedis(host=r['host'], port=r['port'], db=r['db'])
        count = r.incr('esid')
        conf = config['elasticsearch']
        es = pyes.ES("{0}:{1}".format(conf['host'],conf['port']))
        index = {"resource_locator":envelope['resource_locator'],'resource_data':envelope['resource_data'],'doc_ID':envelope['doc_ID']}
        es.index(index,conf['index'],conf['index-type'],count)
@task
def insertDocumentSolr(envelope,config):
        pass