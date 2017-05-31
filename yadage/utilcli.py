import yaml
import json
import click
import tempfile
import shutil
import yadagemodels
import visualize
import logging
from yadagestep import outputReference
from handlers.expression_handlers import handlers as exh
from helpers import set_trivial_backend



def process(x, dag):
    if type(x) == dict:
        for k, v in x.iteritems():
            x[k] = process(v, dag)
        return x
    elif type(x) == list:
        for i, e in enumerate(x):
            x[i] = process(e, dag)
        return x
    elif type(x) == outputReference:
        return x.pointer.resolve(dag.getNode(x.stepid).result)
    else:
        return x


def wflow_with_trivial_backend(instance,results):
    wflow = yadagemodels.YadageWorkflow.fromJSON(
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
        process(selresult, wflow.dag),
        sort_keys=True,
        indent=4,
        separators=(',', ': ')),
        fg='green'
    )


def resolve_wflowref(jsondata):
    if isinstance(jsondata,list):
        for x in jsondata:
            resolve_wflowref(x)
    if isinstance(jsondata,dict):
        for x in jsondata.values():
            resolve_wflowref(x)


@utilcli.command()
@click.argument('instance')
@click.argument('results')
@click.argument('sched')
@click.option('-v', '--verbosity', default='INFO')
def testsched(instance, results, sched,verbosity):


    logging.basicConfig(level=getattr(logging, verbosity), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    wflow = wflow_with_trivial_backend(instance,results)

    data = yaml.load(open(sched))


    import jq
    import jsonpointer
    from handlers.expression_handlers import pointerize
    binds = data['bindings']
    wflowrefs = [jsonpointer.JsonPointer.from_parts(x) for x in jq.jq('paths(if objects then has("$wflowref") else false end)').transform(binds, multiple_output = True)]

    for wflowref in wflowrefs:
        nodeselector, resultscript = wflowref.resolve(binds)['$wflowref']

        view = wflow.view()
        nodes   = [view.dag.getNode(n.get('_nodeid')) for n in jq.jq(nodeselector).transform(view.steps, multiple_output = True)]
        results = [jq.jq(resultscript).transform(pointerize(n.result,False,n.identifier), multiple_output = True) for n in nodes]
        wflowref.set(binds,results)

    stagescript = data['steps']
    stageres = jq.jq(stagescript).transform(binds,multiple_output = False)
    for forstep in stageres:
        wflowpointers = [jsonpointer.JsonPointer.from_parts(x) for x in jq.jq('paths(if objects then has("$wflowpointer") else false end)').transform(forstep, multiple_output = True)]
        for wflowptr in wflowpointers:

            pointer =  wflowptr.resolve(forstep)['$wflowpointer']
            view = wflow.view()
            value = jsonpointer.JsonPointer(pointer['result']).resolve(view.dag.getNode(pointer['step']).result)
            wflowptr.set(forstep,value)

        final = jq.jq(data['post']).transform(forstep)
        print json.dumps(final)
    # print stageres

    # stepscript = data['schedule'][1]

    # stepres = jq.jq(stepscript).transform(stageres)
    # print stepres

    # print results

    # selresult = exh['stage-output-selector'](wflow.view(), yaml.load(selection))

    # if not selresult:
    #     click.secho('Bad selection', fg='red')
    #     return

    # click.secho(json.dumps(
    #     process(selresult, wflow.dag),
    #     sort_keys=True,
    #     indent=4,
    #     separators=(',', ': ')),
    #     fg='green'
    # )


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
