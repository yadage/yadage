#!/usr/bin/env python
import os
import click
import jsonschema
import logging
import workflow_loader
import capschemas
import json

logging.basicConfig(level = logging.ERROR)
log = logging.getLogger(__name__)
@click.command()
@click.argument('workflow')
@click.argument('toplevel', default = '')
@click.argument('schemadir', default = '')
def main(workflow,toplevel,schemadir):
    if not toplevel:
        toplevel = os.getcwd()
    if not schemadir:
        schemadir = capschemas.schemadir
    try:
        data = workflow_loader.workflow(workflow, toplevel = toplevel, schemadir = schemadir, validate = True)
        click.secho('workflow validates against schema', fg = 'green')
    except jsonschema.exceptions.ValidationError:
        log.exception('validation error')
        click.secho('workflow does not validate against schema', fg = 'red')
    except:
        # log.exception('')
        click.secho('this is not even wrong (non-ValidationError exception)', fg = 'red')

        
if __name__ == '__main__':
    main()
