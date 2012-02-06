from datetime import timedelta
config = {
	"lrUrl": "http://alpha.learningregistry.org/harvest/listrecords",
	"mongodb":{	
		"database":"lr",
		"collection":"envelope",
		"host": "localhost",
		"port": 27017,
	},
	"couchdb":{
		"dbUrl":"http://localhost:5984/lr-data"
	},
	'elasticsearch':{
		"host":"localhost",
		"port":9200,
		"index":"lr",
		"index-type":"lr"
	},
	"insertTask":"tasks.save.insertDocumentElasticSearch",
	"validationTask":"tasks.validate.emptyValidate",
	"redis":{
		"host":"localhost",
		"port":6379,
		"db":0
	}
}
# List of modules to import when celery starts.
CELERY_IMPORTS = ("tasks.harvest","tasks.save","tasks.validate")

## Result store settings.
#CELERY_RESULT_BACKEND = "database"
#CELERY_RESULT_DBURI = "sqlite:///mydatabase.db"

## Broker settings.
BROKER_URL = "amqp://guest:guest@localhost:5672//"

## Worker settings
## If you're doing mostly I/O you can have more processes,
## but if mostly spending CPU, try to keep it close to the
## number of CPUs on your machine. If not set, the number of CPUs/cores
## available will be used.
CELERYD_CONCURRENCY = 10

CELERYBEAT_SCHEDULE = {
    "harvestLR": {
        "task": "tasks.harvest.startHarvest",
        "schedule": timedelta(minutes=1),
        "args": (config,)
    },
}
