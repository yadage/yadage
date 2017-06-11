import yaml
import json
import click
import tempfile
import shutil
import visualize
import logging

from wflow import YadageWorkflow
from handlers.expression_handlers import handlers as exh
from helpers import set_trivial_backend, process_refs


def printRef(ref, dag, indent=''):
    click.secho('{}name: {} position: {}, value: {}, id: {}'.format(
        indent,
        dag.getNode(ref.stepid).name,
        ref.pointer.path,
        ref.pointer.resolve(dag.getNode(ref.stepid).result),
        ref.stepid
    ),
        fg='cyan')


def wflow_with_trivial_backend(instance,results):
    wflow = YadageWorkflow.fromJSON(
        json.load(open(instance))
    )
    set_trivial_backend(wflow.dag, json.load(open(results)))
    return wflow

@click.group()
def utilcli():
    pass


@utilcli.command()
@click.argument('instance')
@click.argument('results')
@click.argument('selection')
@click.option('-v', '--verbosity', default='INFO')
def testsel(instance, results, selection,verbosity):


    logging.basicConfig(level=getattr(logging, verbosity), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


    wflow = wflow_with_trivial_backend(instance,results)

    selresult = exh['stage-output-selector'](wflow.view(), yaml.load(selection))

    if not selresult:
        click.secho('Bad selection', fg='red')
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
@click.argument('results')
@click.argument('vizpdf')
def viz(instance, results, vizpdf):
    wflow = wflow_with_trivial_backend(instance,results)

    dirpath = tempfile.mkdtemp()
    visualize.write_prov_graph(dirpath, wflow)
    shutil.copy('{}/yadage_workflow_instance.pdf'.format(dirpath), vizpdf)
    shutil.rmtree(dirpath)


if __name__ == '__main__':
    utilcli()
