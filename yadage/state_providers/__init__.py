import os
import importlib
import logging
from .localposix import LocalFSProvider

log = logging.getLogger(__name__)

def load_provider(jsondata,deserialization_opts = None):

    log.debug('load_provider opts %s', deserialization_opts)
    deserialization_opts = deserialization_opts or {}
    if jsondata == None:
        return None
    if 'state_provider' in deserialization_opts:
        providerstring = deserialization_opts.get('state_provider','')
        if providerstring.startswith('py:'):
            _, module, providerclass = providerstring.split(':')
            module = importlib.import_module(module)
            providerclass = getattr(module,providerclass)
            provideropts = {}
            return providerclass.fromJSON(jsondata,**provideropts)
    if 'YADAGE_STATEPROVIDER' in os.environ:
        module = importlib.import_module(os.environ['YADAGE_STATEPROVIDER'])
        return module.load_provider(jsondata)
    if jsondata['state_provider_type'] == 'localfs_provider':
        return LocalFSProvider.fromJSON(jsondata, deserialization_opts)
    raise TypeError('unknown provider type {}'.format(jsondata['state_provider_type']))
