import json
import urllib2
import urllib
import urlparse
import os
from pymongo import Connection
from celery.task import task
from celery.execute import send_task
from datetime import datetime
from celery.log import get_default_logger
import redis
log = get_default_logger()
@task
def startHarvest(config):
    lrUrl = config['lrUrl']
    r = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'], db=config['redis']['db'])
    fromDate = None 
    try:
        fromDate = r.get('lastHarvestTime')
    except:
        pass    
    until = datetime.utcnow().isoformat()+"Z"
    r.set("lastHarvestTime",until)
    urlParts = urlparse.urlparse(lrUrl)
    params = {"until":until}    
    if fromDate is not  None:
        params['from'] = fromDate
    newQuery = urllib.urlencode(params)
    lrUrl = urlparse.urlunparse((urlParts[0],
                                 urlParts[1],
                                 urlParts[2],
                                 urlParts[3],
                                 newQuery,
                                 urlParts[5]))                               
    harvestData.delay(lrUrl,config)
    
@task
def harvestData(lrUrl , config):    
    resp = urllib2.urlopen(lrUrl)
    data = json.load(resp)  
    for i in data['listrecords']:
        envelope =  i['record']['resource_data']
        send_task(config['validationTask'],[envelope,config])       
    if data.has_key("resumption_token") and \
       data['resumption_token'] is not None and \
       data['resumption_token'] != "null":
        urlParts = urlparse.urlparse(lrUrl)
        newQuery = urllib.urlencode({"resumption_token":data['resumption_token']})
        lrUrl = urlparse.urlunparse((urlParts[0],
                                     urlParts[1],
                                     urlParts[2],
                                     urlParts[3],
                                     newQuery,
                                     urlParts[5]))
        harvestData.delay(lrUrl,config)                                     
@task
def emptyValidate(envelope,config):
    raise Exception("Got Here")
    send_task(config['insertTask'],[envelope,config])        
@task
def insertDocumentMongo(envelope, config):
    try:
        con = Connection(config['host'],config['port'])
        db = con[config['database']]
        collection = db[config['collection']]   
        del envelope['_rev']
        del envelope['_id']
        collection.insert(envelope)
    except (Exception), exc:
        log.error("Error writing to mongo")
        processHarvestResult.retry(exc)    