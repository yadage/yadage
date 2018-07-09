import os
import importlib
import logging
from .localposix import LocalFSProvider

log = logging.getLogger(__name__)

from ..handlers.utils import handler_decorator

provider_deserialize_handlers, provider_deserializer = handler_decorator()

@provider_deserializer('localfs_provider')
def localfs_provider(jsondata, deserialization_opts):
    return LocalFSProvider.fromJSON(jsondata, deserialization_opts)

@provider_deserializer('frompython_provider')
def frompython_provider(jsondata, deserialization_opts):
    providerstring = deserialization_opts.get('state_provider','')
    _, module, providerclass = providerstring.split(':')
    module = importlib.import_module(module)
    providerclass = getattr(module,providerclass)
    provideropts = {}
    return providerclass.fromJSON(jsondata,**provideropts)

@provider_deserializer('fromenv_provider')
def fromenv_provider(jsondata, deserialization_opts):
    module = importlib.import_module(os.environ['YADAGE_STATEPROVIDER'])
    return module.load_provider(jsondata)

def load_provider(jsondata,deserialization_opts = None):
    log.debug('load_provider opts %s', deserialization_opts)
    deserialization_opts = deserialization_opts or {}
    if jsondata == None:
        return None
    if 'state_provider' in deserialization_opts:
        providerstring = deserialization_opts.get('state_provider','')
        if providerstring.startswith('py:'):
            return provider_deserialize_handlers['frompython_provider'](jsondata, deserialization_opts)
    if 'YADAGE_STATEPROVIDER' in os.environ:
        return provider_deserialize_handlers['fromenv_provider'](jsondata, deserialization_opts)

    return provider_deserialize_handlers[jsondata['state_provider_type']](jsondata, deserialization_opts)

    raise TypeError('unknown provider type {}'.format(jsondata['state_provider_type']))
