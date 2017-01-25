import yaml
import os
import click
import zipfile
import urllib
import logging
log = logging.getLogger(__name__)

def discover_initfiles(initdata,sourcedir):
    '''inspect sourcedir '''
    log.info('inspecting %s to discover referenced input files',sourcedir)
    for k, v in initdata.iteritems():
        if type(v) not in [unicode,str]: continue
        candpath = '{}/{}'.format(sourcedir, v)
        if os.path.exists(candpath):
            initdata[k] = candpath
    return initdata

def getinit_data(initfiles, parameters):
    '''
    get initial data from both a list of files and a list of 'pname=pvalue'
    strings as they are passed in the command line <pvalue> is assumed to be a
    YAML parsable string.
    '''
    initdata = {}
    for initfile in initfiles:
        log.info('loading initialization data from file %s',initfile)
        initdata.update(**yaml.load(open(initfile)))

    for x in parameters:
        key, value = x.split('=')
        initdata[key] = yaml.load(value)
    return initdata


def prepare_workdir_from_archive(workdir, inputarchive):
    if os.path.exists(workdir):
        raise click.exceptions.ClickException(click.style(
            "workdirectory exists and input archive give. Can't have both", fg='red'))
    initdir = os.path.join(workdir,'init')
    os.makedirs(initdir)
    localzipfile = '{}/inputarchive.zip'.format(workdir)
    urllib.urlretrieve(inputarchive, localzipfile)
    with zipfile.ZipFile(localzipfile) as zf:
        zf.extractall(path=initdir)
    os.remove(localzipfile)
    return initdir


def setupbackend_fromstring(backend, name = 'backendname', cacheconfig=None):
    import backends.packtivitybackend as pb
    backend = pb.PacktivityBackend(packtivity_backendstring = backend, cacheconfig = cacheconfig)
    return backend
