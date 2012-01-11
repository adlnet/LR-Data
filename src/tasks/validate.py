from celery.task import task
from celery.execute import send_task
from celery.log import get_default_logger
log = get_default_logger()

@task
def emptyValidate(envelope,config):
    send_task(config['insertTask'],[envelope,config])        