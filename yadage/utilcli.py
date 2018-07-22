import json
import logging
import shutil
import tempfile

import click
import yaml

from .handlers.expression_handlers import handlers as exh
from .utils import process_refs
from .wflow import YadageWorkflow


def printRef(ref, dag, indent=''):
    click.secho('{}name: {} position: {}, value: {}, id: {}'.format(
        indent,
        dag.getNode(ref.stepid).name,
        ref.pointer.path,
        ref.pointer.resolve(dag.getNode(ref.stepid).result),
        ref.stepid
    ),
        fg='cyan')

def wflow_with_trivial_backend(instance):

    stateopts = {}
    wflow = YadageWorkflow.fromJSON(json.load(open(instance)),stateopts)
    return wflow

@click.group()
def utilcli():
    pass


@utilcli.command()
@click.argument('instance')
@click.argument('selection')
@click.option('--viewscope', default = '')
@click.option('-v', '--verbosity', default='INFO')
def testsel(instance, selection,verbosity,viewscope):


    logging.basicConfig(level=getattr(logging, verbosity), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


    wflow = wflow_with_trivial_backend(instance)

    selresult = exh['stage-output-selector'](wflow.view(viewscope), yaml.load(selection))

    if not selresult:
        click.secho('Bad selection {}'.format(selresult), fg='red')
        return

    click.secho(json.dumps(
        process_refs(selresult, wflow.dag),
        sort_keys=True,
        indent=4,
        separators=(',', ': ')),
        fg='green'
    )


@utilcli.command()
@click.argument('instance')
@click.argument('vizpdf')
@click.option('--viewscope', default = '')
@click.option('-v', '--verbosity', default='INFO')
def viz(instance, vizpdf,viewscope,verbosity):
    logging.basicConfig(level=getattr(logging, verbosity), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    import yadage.visualize as visualize
    wflow = wflow_with_trivial_backend(instance)

    dirpath = tempfile.mkdtemp()
    visualize.write_prov_graph(dirpath, wflow, scope = viewscope)
    shutil.copy('{}/yadage_workflow_instance.pdf'.format(dirpath), vizpdf)
    shutil.rmtree(dirpath)


if __name__ == '__main__':
    utilcli()
