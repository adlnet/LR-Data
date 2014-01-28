from tasks.harvest import *
from celeryconfig import config
from redis import StrictRedis
r = StrictRedis(db=0)
r.set("lastHarvestTime", "2014-01-22T11:00:00Z")
startHarvest(config)
