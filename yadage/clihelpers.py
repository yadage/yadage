import yaml
import os
import click
import zipfile
import urllib2
import logging
import jsonpointer
import jq
import glob2 as glob

log = logging.getLogger(__name__)


def leaf_iterator(jsonable):
    '''
    generator function to yield leafs items of a JSON-like structure alongside
    their position in the structure as determined by a JSONPointer.
    
    :return: tuples (jsonpointer, leaf value)
    '''
    allleafs = jq.jq('leaf_paths').transform(jsonable, multiple_output = True)
    leafpointers = [jsonpointer.JsonPointer.from_parts(x) for x in allleafs]
    for x in leafpointers:
        yield x,x.get(jsonable)

def discover_initfiles(initdata,sourcedir):
    '''inspect sourcedir, first tries exact path match, and then (possbly recursive) glob'''
    log.info('inspecting %s to discover referenced input files',sourcedir)

    # filled_initdata = copy.deepcopy(initdata)
    for pointer,value in leaf_iterator(initdata):
        if type(value) not in [str,unicode]: continue
        within_sourcedir = os.path.join(sourcedir,value)
        globresult = glob.glob(os.path.join(sourcedir,value))
        if os.path.exists(within_sourcedir):
            pointer.set(initdata,within_sourcedir)
        elif globresult:
            pointer.set(initdata,globresult)
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
    f = urllib2.urlopen(inputarchive)
    with open(localzipfile,'w') as lf:
        lf.write(f.read())
    with zipfile.ZipFile(localzipfile) as zf:
        zf.extractall(path=initdir)
    os.remove(localzipfile)
    return initdir

def setupbackend_fromstring(backend, name = 'backendname', cacheconfig=None):
    import backends.packtivitybackend as pb
    backend = pb.PacktivityBackend(packtivity_backendstring = backend, cacheconfig = cacheconfig)
    return backend
