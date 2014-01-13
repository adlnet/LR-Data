from tasks.harvest import *
from celeryconfig import config
from redis import StrictRedis
r = StrictRedis(db=0)
startHarvest(config)
