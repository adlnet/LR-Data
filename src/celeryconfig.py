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
    }
}
# List of modules to import when celery starts.
CELERY_IMPORTS = ("tasks.harvest", "tasks.save", "tasks.validate", )

## Result store settings.
CELERY_RESULT_BACKEND = "redis"
CELERY_RESULT_BACKEND = "redis://localhost/2"
CELERY_TASK_RESULT_EXPIRES = 15
## Broker settings.
BROKER_URL = "redis://localhost:6379/3"

CELERY_LOG_DEBUG = "TRUE"
CELERY_LOG_FILE = "./celeryd.log"
CELERY_LOG_LEVEL = "INFO"
BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 600}
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
