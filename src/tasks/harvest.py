import json
import urllib2
import urllib
from urlparse import urlparse, urlunparse
import redis
from datetime import datetime
from celery.task import task
from celery.log import get_default_logger
from .validate import *
log = get_default_logger()


@task(queue="harvest")
def startHarvest(config):
    log.debug('got here')
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
    return lrUrl


@task(queue="harvest")
def harvestData(lrUrl, config):
    try:
        resp = urllib2.urlopen(lrUrl)
        data = json.load(resp)
        for i in data['listrecords']:
            envelope = i['record']['resource_data']
            checkWhiteList.delay(envelope, config)
        if "resumption_token" in data and \
           data['resumption_token'] is not None and \
           data['resumption_token'] != "null":
            urlParts = urlparse(lrUrl)
            newQuery = urllib.urlencode({"resumption_token": data['resumption_token']})
            lrUrl = urlunparse((urlParts[0],
                                urlParts[1],
                                urlParts[2],
                                urlParts[3],
                                newQuery,
                                urlParts[5]))
            harvestData.delay(lrUrl, config)
    except Exception as ex:
        harvestData.delay(lrUrl, config)
