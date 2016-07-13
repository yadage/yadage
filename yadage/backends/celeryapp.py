from celery import Celery
import os
redis_string = 'redis://{hostname}:{port}/{database}'.format(
  hostname = os.environ.get('YADAGE_REDIS_HOST','localhost'),
  port = os.environ.get('YADAGE_REDIS_PORT',6379),
  database = os.environ.get('YADAGE_REDIS_DB',0)
)
app = Celery(broker = redis_string, backend = redis_string)
