#!/usr/bin/env python
import os
import click
import jsonschema
import logging
import workflow_loader
import yadageschemas
import json
import jsonref


class ref(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, jsonref.JsonRef):
            return obj.copy()
        return json.JSONEncoder.default(self, obj)


logging.basicConfig(level=logging.ERROR)
log = logging.getLogger(__name__)


@click.command()
@click.argument('workflow')
@click.argument('toplevel', default='')
@click.argument('schemadir', default='')
@click.option('--stdout', '-s', default=False, is_flag=True)
def main(workflow, toplevel, schemadir, stdout):
    if not toplevel:
        toplevel = os.getcwd()
    if not schemadir:
        schemadir = yadageschemas.schemadir
    try:
        data = workflow_loader.workflow(
            workflow, toplevel=toplevel, schemadir=schemadir, validate=True)
        if stdout:
            print json.dumps(data, cls=ref)
        else:
            click.secho('workflow validates against schema', fg='green')
    except jsonschema.exceptions.ValidationError:
        log.exception('validation error')
        click.secho('workflow does not validate against schema', fg='red')
    except:
        log.exception('')
        click.secho(
            'this is not even wrong (non-ValidationError exception)', fg='red')


if __name__ == '__main__':
    main()
