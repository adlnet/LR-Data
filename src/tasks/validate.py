from celery.task import task
from celery.execute import send_task
from celery.log import get_default_logger
log = get_default_logger()
from pybloomfilter import BloomFilter
from urlparse import urlparse


@task
def emptyValidate(envelope, config):
    send_task(config['insertTask'], [envelope, config])


@task
def checkWhiteList(envelope, config):
    bf = BloomFilter.open("filter.bloom")
    parts = urlparse(envelope['resource_location'])
    if parts.netloc in bf:
        send_task(config['insertTask'], [envelope, config])
