from celery.task import task
from celery.execute import send_task
from celery.log import get_default_logger
log = get_default_logger()
from pybloomfilter import BloomFilter
from urlparse import urlparse
import requests
black_list = set(["bit.ly", "goo.gl", "tinyurl.com", "fb.me", "j.mp", "su.pr"])


@task
def emptyValidate(envelope, config):
    send_task(config['insertTask'], [envelope, config])


@task
def checkWhiteList(envelope, config):
    bf = BloomFilter.open("filter.bloom")
    parts = urlparse(envelope['resource_locator'])
    if parts.netloc in bf and parts.netloc not in black_list:
    	save = True
    	try:
    		resp = requests.get(envelope['resource_location'])
    		if resp.status_code != requests.codes.ok:
    			save = False
    	except:
    		save = False
    	if save:
        	send_task(config['insertTask'], [envelope, config])
