import yaml
import os
import click
import zipfile
import psutil
import urllib

def getinit_data(initfiles,parameters):
    '''
    get initial data from both a list of files and a list of 'pname=pvalue'
    strings as they are passed in the command line <pvalue> is assumed to be a
    YAML parsable string.
    '''
    initdata = {}
    for initfile in initfiles:
        initdata.update(**yaml.load(open(initfile)))

    for x in parameters:
        key,value = x.split('=')
        initdata[key]=yaml.load(value)
    return initdata

def prepare_workdir_from_archive(workdir,inputarchive):
    if os.path.exists(workdir):
        raise click.exceptions.ClickException(click.style("workdirectory exists and input archive give. Can't have both", fg = 'red'))
    inputdata = '{}/inputs'.format(workdir)
    os.makedirs(inputdata)
    localzipfile = '{}/inputarchive.zip'.format(workdir)
    urllib.urlretrieve(inputarchive,localzipfile)
    with zipfile.ZipFile(localzipfile) as zf:
        zf.extractall(path = inputdata)
    os.remove(localzipfile)

def setupbackend_fromstring(backend, name = 'backendname'):
    if backend.startswith('multiproc'):
        import backends.packtivitybackend as pb
        nparallel  = backend.split(':')[1]
        if nparallel == 'auto':
            nparallel = psutil.cpu_count()
        else:
            nparallel = int(nparallel)
        backend = pb.PacktivityMultiProcBackend(nparallel)
    elif backend == 'celery':
        import backends.celeryapp
        import backends.packtivity_celery as pc
        backend = pc.PacktivityCeleryBackend(backends.celeryapp.app)
    elif backend == 'foreground':
        import backends.packtivitybackend as pb
        backend = pb.PacktivityForegroundBackend()
    elif backend == 'jira':
        import backends.jira as jb
        backend = jb.JiraBackend('workflow request - {}'.format(name),'some description')
    else:
        raise NotImplementedError('backend config {} not known'.format(backend))

    return backend
