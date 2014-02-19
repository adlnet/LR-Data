import json
import urllib2
import urllib
from urlparse import urlparse, urlunparse
import redis
from datetime import datetime
from celery.task import task
from celery.log import get_default_logger
from .validate import *
import traceback
log = get_default_logger()


@task(queue="harvest")
def startHarvest(config):
    try:
        lrUrl = config['lrUrl']
        r = redis.StrictRedis(host=config['redis']['host'],
                              port=config['redis']['port'],
                              db=config['redis']['db'])
        fromDate = None
        try:
            fromDate = r.get('lastHarvestTime')
        except:
            pass
        until = datetime.utcnow().isoformat() + "Z"
        r.set("lastHarvestTime", until)
        urlParts = urlparse(lrUrl)
        params = {"until": until}
        if fromDate is not None:
            params['from'] = fromDate
        newQuery = urllib.urlencode(params)
        lrUrl = urlunparse((urlParts[0],
                            urlParts[1],
                            urlParts[2],
                            urlParts[3],
                            newQuery,
                            urlParts[5]))
        print(lrUrl)
        harvestData.delay(lrUrl, config)
    except Exception as ex:
        traceback.print_exc()
        startHarvest.retry(exc=ex)


@task(queue="harvest")
def harvestData(lrUrl, config):
    try:
        r = redis.StrictRedis(host=config['redis']['host'],
                              port=config['redis']['port'],
                              db=config['redis']['db'])        
        resp = urllib2.urlopen(lrUrl)
        data = json.load(resp)
        for i in data['listrecords']:
            envelope = i['record']['resource_data']
            r.sadd("docs", envelope['doc_ID'])
            checkWhiteList.delay(envelope, config)
        if "resumption_token" in data and \
           data['resumption_token'] is not None and \
           data['resumption_token'] != "null":
            urlParts = urlparse(lrUrl)
            rawQuery = {"resumption_token": data['resumption_token']}
            newQuery = urllib.urlencode(rawQuery)
            lrUrl = urlunparse((urlParts[0],
                                urlParts[1],
                                urlParts[2],
                                urlParts[3],
                                newQuery,
                                urlParts[5]))
            harvestData.delay(lrUrl, config)
    except Exception as exc:
        traceback.print_exc()
        harvestData.retry(exc=exc)
