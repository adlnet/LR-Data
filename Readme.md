# LR-Data
This is a small utility to help pull the data from the Learning Registry into a datastore of you choice.

# Dependencies
## LR-Data requires 
###RabbitMQ
###Redis
###Python
###Celery

#Setup
Run `pip install -U -r requirements.txt`

#Configuration
All configuration is done in the src/celeryconfig.py file.  For information of configuring Celery please see their [document](http://celery.readthedocs.org/en/latest/index.html).  For lr-data configuration modify 

    config = {

	"lrUrl": "http://lrdev02.learningregistry.org/harvest/listrecords",

	"mongodb":{	

		"database":"lr",

		"collection":"envelope",

		"host": "localhost",

		"port": 27017,

	},

	"couchdb":{

		"dbUrl":"http://localhost:5984/lr-data"

	},

	"insertTask":"tasks.save.insertDocumentMongo",

	"validationTask":"tasks.validate.emptyValidate",

	"redis":{

		"host":"localhost",

		"port":6379,

		"db":0

	}

    }

set `insertTask` to be the celery task you wish to use to save the data and modify `validationTask` to be your validation task
#Startup
To start run `celryd -B` from the source directory.  To run as a deamon follow these [instructions](http://ask.github.com/celery/cookbook/daemonizing.html)
