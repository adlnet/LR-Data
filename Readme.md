# LR-Data
This is a small utility to help pull the data from the Learning Registry into a datastore of your choice.

# Dependencies
LR-Data requires: 
* [RabbitMQ](http://www.rabbitmq.com/)
* [Redis](http://redis.io/)
* [Python](http://www.python.org/)
* [Celery](http://www.celeryproject.org/)

### Platform-Specific Requirements:
On OS X, you will also need `libevent`, which can be installed with homebrew: `brew install libevent`

#Setup
Run `pip install -U -r requirements.txt`

#Configuration
All configuration is done in the src/celeryconfig.py file.  For information of configuring Celery please see their [document](http://celery.readthedocs.org/en/latest/index.html).  For lr-data configuration modify 

`config = {

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

}`

set `insertTask` to be the celery task you wish to use to save the data and modify `validationTask` to be your validation task
#Startup
To start run `celryd -B` from the source directory.  To run as a deamon follow these [instructions](http://ask.github.com/celery/cookbook/daemonizing.html)

## Contributing to the project
We welcome contributions to this project. Fork this repository, make changes, and submit pull requests. If you're not comfortable with editing the code, please [submit an issue](https://github.com/adlnet/LR-Data/issues) and we'll be happy to address it. 

## License
   Copyright &copy;2016 Advanced Distributed Learning

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

