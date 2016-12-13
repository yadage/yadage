import yaml
import os
import click
import zipfile
import urllib


def getinit_data(initfiles, parameters):
    '''
    get initial data from both a list of files and a list of 'pname=pvalue'
    strings as they are passed in the command line <pvalue> is assumed to be a
    YAML parsable string.
    '''
    initdata = {}
    for initfile in initfiles:
        initdata.update(**yaml.load(open(initfile)))

    for x in parameters:
        key, value = x.split('=')
        initdata[key] = yaml.load(value)
    return initdata


def prepare_workdir_from_archive(workdir, inputarchive):
    if os.path.exists(workdir):
        raise click.exceptions.ClickException(click.style(
            "workdirectory exists and input archive give. Can't have both", fg='red'))
    inputdata = '{}/init'.format(workdir)
    os.makedirs(inputdata)
    localzipfile = '{}/inputarchive.zip'.format(workdir)
    urllib.urlretrieve(inputarchive, localzipfile)
    with zipfile.ZipFile(localzipfile) as zf:
        zf.extractall(path=inputdata)
    os.remove(localzipfile)


def setupbackend_fromstring(backend, name = 'backendname', cacheconfig=None):
    import backends.packtivitybackend as pb
    backend = pb.PacktivityBackend(packtivity_backendstring = backend, cacheconfig = cacheconfig)
    return backend
