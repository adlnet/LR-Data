import couchdb
from pymongo import Connection
from celery.task import task
from celery.log import get_default_logger
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
def insertDataCouchdb(envelope,config):
    try:
        conf = config['couchdb']
        db = couchdb.Database(conf['dbUrl'])
        del envelope['_rev']
        del envelope['_id']
        db.save(envelope)
    except (Exception), exc:
        log.error("Error writing to mongo")
        processHarvestResult.retry(exc)        