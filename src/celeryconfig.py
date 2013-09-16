from datetime import timedelta
config = {
    "lrUrl": "https://node01.public.learningregistry.net/harvest/listrecords",
    "couchdb": {
        "dbUrl": "http://localhost:5984/lr-data",
        "standardsDb": "http://localhost:5984/standards",
    },
    "insertTask": "tasks.save.createRedisIndex",
    "validationTask": "tasks.validate.checkWhiteList",
    "redis": {
        "host": "localhost",
        "port": 6379,
        "db": 0
    },
    "riak": {
        "port": "10017",
        "host": "localhost",
        "protocol": "pbc"
    }
}
# List of modules to import when celery starts.
CELERY_IMPORTS = ("tasks.harvest", "tasks.save", "tasks.validate", "tasks.display", "tasks.index", "tasks.parse")

## Result store settings.
CELERY_RESULT_BACKEND = 'amqp://'
CELERY_TASK_RESULT_EXPIRES = 15
## Broker settings.
BROKER_URL = 'redis://localhost:6379/2'

CELERY_LOG_DEBUG = "TRUE"
CELERY_LOG_FILE = "./celeryd.log"
CELERY_LOG_LEVEL = "DEBUG"
BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 3600}
CELERY_RESULT_BACKEND = "redis://localhost/1"
CELERY_TASK_RESULT_EXPIRES = 1
## Worker settings
## If you're doing mostly I/O you can have more processes,
## but if mostly spending CPU, try to keep it close to the
## number of CPUs on your machine. If not set, the number of CPUs/cores
## available will be used.

CELERYBEAT_SCHEDULE = {
    "harvestLR": {
        "task": "tasks.harvest.startHarvest",
        "schedule": timedelta(hours=1),
        "args": (config,)
    },
}
