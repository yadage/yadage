#!/usr/bin/env python

import yadage.backends.packtivity_celery
import yadage.backends.celeryapp
import yadage.yadagemodels
import json
import logging
logging.basicConfig(level = logging.INFO)

import click



@click.command()
@click.argument('statefile')
def main(statefile):
    pass


    backend = yadage.backends.packtivity_celery.PacktivityCeleryBackend(
        yadage.backends.celeryapp.app
    )

    workflow = yadage.yadagemodels.YadageWorkflow.fromJSON(
        json.load(open(statefile)),
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
        click.secho('stepping workflow', fg = 'green')
        coroutine.next()
        click.secho('saving new state', fg = 'green')
        with open(statefile,'w') as f:
            json.dump(workflow.json(),f)


    except StopIteration:
        click.secho('workflow done.', fg = 'green')

if __name__ == '__main__':
    main()
