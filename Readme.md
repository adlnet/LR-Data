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

#Startup
To start run `celryd -B` from the source directory.  To run as a deamon follow these [instructions](http://ask.github.com/celery/cookbook/daemonizing.html)