#!/usr/bin/env python
import click
import jsonschema
from workflow_loader import validate_workflow
import logging
logging.basicConfig(level = logging.DEBUG)
log = logging.getLogger(__name__)
@click.command()
@click.argument('workflow')
@click.argument('toplevel')
@click.argument('schemadir')
def main(workflow,toplevel,schemadir):
    try:
        validate_workflow(workflow, toplevel = toplevel, schemadir = schemadir)
        click.secho('workflow validates against schema', fg = 'green')
    except jsonschema.exceptions.ValidationError:
        log.exception('validation error')
        click.secho('workflow does not validate against schema', fg = 'red')
        
if __name__ == '__main__':
    main()
