import json
import logging
import shutil
import tempfile
import os
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

@utilcli.group()
def k8s():
    pass

@k8s.command()
@click.option('--hostname', default = 'docker-for-desktop')
@click.option('--path', default = None)
def create_state(hostname,path):
    pvc_name = 'yadagedata'
    sc_name = 'local-storage'
    path_base = path or os.getcwd()
    size = '1G'
    kubeyaml = '''\
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: {pvc_name}
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: {size}
  storageClassName: {sc_name}
---
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: {sc_name}
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: yadage-pv
spec:
  capacity:
    storage: {size}
  volumeMode: Filesystem
  accessModes:
  - ReadWriteMany
  persistentVolumeReclaimPolicy: Delete
  storageClassName: {sc_name}
  local:
    path: {path_base}
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
          - {hostname}
'''.format(
        pvc_name = pvc_name,
        sc_name = sc_name,
        base_path = path_base,
        size = size,
        path_base = path_base,
        hostname = hostname
    )
    click.echo(kubeyaml)

if __name__ == '__main__':
    utilcli()
