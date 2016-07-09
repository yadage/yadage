STATEFILE = 'manual_instance.json'
import yadage.backends.packtivity_celery
import yadage.backends.celeryapp
import yadage.yadagemodels
import json
import logging
logging.basicConfig(level = logging.INFO)

backend = yadage.backends.packtivity_celery.PacktivityCeleryBackend(
    yadage.backends.celeryapp.app
)

workflow = yadage.yadagemodels.YadageWorkflow.fromJSON(
    json.load(open(STATEFILE)),
    yadage.backends.packtivity_celery.PacktivityCeleryProxy,
    backend
)

import adage
ym = adage.yes_man()
ym.next() #prime decider
coroutine = adage.adage_coroutine(backend,ym)
coroutine.next() #prime the coroutine....
coroutine.send(workflow)

try:
    print 'step'
    coroutine.next()
    with open(STATEFILE,'w') as f:
        json.dump(workflow.json(),f)


except StopIteration:
    print 'done'
