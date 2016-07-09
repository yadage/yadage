#!/usr/bin/env python

import yadage.backends.packtivity_celery
import yadage.backends.celeryapp
import yadage.workflow_loader
import yadage.yadagemodels
import os
import yaml
import json


import click


@click.command()
@click.argument('workdir')
@click.argument('workflow')
@click.argument('initdata', default = '')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-s','--statefile', default = 'manual_instance.json')
def main(workdir,workflow,initdata,statefile,toplevel):
    
    workflow_def = yadage.workflow_loader.workflow(
        toplevel = toplevel,
        source = workflow
    )

    rootcontext = {
        'readwrite': [os.path.abspath(workdir)],
        'readonly': []
    }

    workflow = yadage.yadagemodels.YadageWorkflow.createFromJSON(workflow_def,rootcontext)

    initdata = yaml.load(open(initdata)) if initdata else {}
    workflow.view().init(initdata)

    with open(statefile,'w') as f:
        json.dump(workflow.json(),f)

    click.secho('initialized workflow', fg = 'green')

if __name__ == '__main__':
    main()
