from tasks.harvest import *
from celeryconfig import config
from redis import StrictRedis
r = StrictRedis(db=0)
r.set("lastHarvestTime", "2013-09-24T19:00:00Z")
startHarvest(config)
