#!/usr/bin/env python
import os
import capschemas
import click
import jsonschema
import workflow_loader
import logging

logging.basicConfig(level = logging.INFO)
log = logging.getLogger(__name__)
@click.command()
@click.argument('workflow')
@click.argument('toplevel', default = None)
@click.argument('schemadir', default = capschemas.schemadir)
def main(workflow,toplevel,schemadir):
    if not toplevel:
        toplevel = os.getcwd()
    try:
        data = workflow_loader.workflow(workflow, toplevel = toplevel, schemadir = schemadir, validate = True)
        click.secho('workflow validates against schema', fg = 'green')
    except jsonschema.exceptions.ValidationError:
        log.exception('validation error')
        click.secho('workflow does not validate against schema', fg = 'red')
    except:
        click.secho('this is not even wrong (non-ValidationError exception)', fg = 'red')

        
if __name__ == '__main__':
    main()
