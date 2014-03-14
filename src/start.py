#! /usr/bin/env python
from tasks.harvest import *
from celeryconfig import config
from redis import StrictRedis
r = StrictRedis(db=0)
# r.set('lastHarvestTime', '2014-01-07T20:58:48Z')
r.delete("lastHarvestTime")
startHarvest(config)
