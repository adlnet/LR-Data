from celery.task import task
from .save import createRedisIndex
from celery.log import get_default_logger
log = get_default_logger()
from pybloomfilter import BloomFilter
from urlparse import urlparse, urlunparse
from urllib import urlencode
import requests
import re
import redis
black_list = set(["bit.ly", "goo.gl", "tinyurl.com", "fb.me", "j.mp", "su.pr", 'www.freesound.org'])
good_codes = [requests.codes.ok, requests.codes.moved, requests.codes.moved_permanently]

# @task(queue="validate")
# def emptyValidate(envelope, config):
#     send_task(config['insertTask'], [envelope, config])


def translate_url(url_parts):
    r = re.compile("\w+:\d+")
    path = url_parts.path
    content_object_id = r.findall(path)[0]
    new_url_parts = ("https", url_parts.netloc, "Public/Model.aspx", url_parts.params, urlencode({"ContentObjectID": content_object_id}), None)
    return urlunparse(new_url_parts)


@task(queue="validate")
def checkWhiteList(envelope, config):
    bf = BloomFilter.open("filter.bloom")
    parts = urlparse(envelope['resource_locator'])
    r = redis.StrictRedis(host=config['redis']['host'],
                          port=config['redis']['port'],
                          db=config['redis']['db'])
    if "lr-test-data-slice-jbrecht" in envelope['keys']:
        return
    if parts.netloc == "3dr.adlnet.gov":
        envelope['resource_locator'] = translate_url(parts)
    if parts.netloc not in bf:
        print('not in whitelist')
        #return
    if parts.netloc in black_list:
        print('blacklist')
        #return 
    try:
        resp = requests.get(envelope['resource_locator'])
        print(envelope['resource_locator'])
        print(resp.status_code)
        if resp.status_code not in good_codes:
            return 
    except Exception as ex:
        print(ex)
        return 
    createRedisIndex.delay(envelope, config)
