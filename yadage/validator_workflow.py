#!/usr/bin/env python
import os
import click
import jsonschema
import logging
import workflow_loader
import yadageschemas
import json
from helpers import WithJsonRefEncoder


logging.basicConfig(level=logging.ERROR)
log = logging.getLogger(__name__)


@click.command()
@click.argument('workflow')
@click.argument('toplevel', default='')
@click.argument('schemadir', default='')
@click.option('--stdout', '-s', default=False, is_flag=True)
def main(workflow, toplevel, schemadir, stdout):
    rc = 1
    if not toplevel:
        toplevel = os.getcwd()
    if not schemadir:
        schemadir = yadageschemas.schemadir
    try:
        data = workflow_loader.workflow(
            workflow, toplevel=toplevel, schemadir=schemadir, validate=True)
        if stdout:
            print json.dumps(data, cls=WithJsonRefEncoder)
        else:
            click.secho('workflow validates against schema', fg='green')
        rc = 0
    except jsonschema.exceptions.ValidationError:
        click.secho('workflow does not validate against schema', fg='red')
        raise
    except:
        log.exception('')
        click.secho(
            'this is not even wrong (non-ValidationError exception)', fg='red')

    if rc == 1:
        exc = click.exceptions.ClickException(
            click.style("validation failed", fg='red'))
        exc.exit_code = rc
        raise exc

if __name__ == '__main__':
    main()
