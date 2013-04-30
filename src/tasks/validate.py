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
black_list = set(["bit.ly", "goo.gl", "tinyurl.com", "fb.me", "j.mp", "su.pr"])
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
    if parts.netloc == "3dr.adlnet.gov":
        envelope['resource_locator'] = translate_url(parts)
    if (parts.netloc in bf and parts.netloc not in black_list):
        save = True
        try:
            resp = requests.get(envelope['resource_locator'])

            if resp.status_code not in good_codes:
                save = False
        except Exception as ex:
            log.exception(ex)
            save = False
        if save:
            r = redis.StrictRedis(host=config['redis']['host'],
                                  port=config['redis']['port'],
                                  db=config['redis']['db'])
            r.sadd("doc_ids", envelope['doc_ID'])
            print(envelope['node_timestamp'])
            createRedisIndex.delay(envelope, config)
            # send_task(config['insertTask'], [envelope, config])
        else:
            print("Filtered: " + envelope['resource_locator'])
