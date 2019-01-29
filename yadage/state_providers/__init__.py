import os
import importlib
import logging
from .localposix import LocalFSProvider

log = logging.getLogger(__name__)

from ..handlers.utils import handler_decorator

provider_deserialize_handlers, provider_deserializer = handler_decorator()

@provider_deserializer('localfs_provider')
def localfs_provider_deserializer(jsondata, deserialization_opts):
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
def fromenv_provider_deserializer(jsondata, deserialization_opts):
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

providersetup_handlers, providersetup = handler_decorator()

@providersetup('local')
def localfs_provider(dataarg,dataopts):
    import yadage.state_providers.localposix
    return yadage.state_providers.localposix.setup_provider(dataarg,dataopts)

@providersetup('py:')
def formpython_provider(dataarg,dataopts):
    _,module, setupfunc,dataarg = dataarg.split(':',3)
    module = importlib.import_module(module)
    setupfunc = getattr(module,setupfunc)
    return setupfunc(dataarg,dataopts)

@providersetup('fromenv:')
def fromenv_provider(dataarg,dataopts):
    module = importlib.import_module(os.environ['PACKTIVITY_STATEPROVIDER'])
    return module.setup_provider(dataarg,dataopts)

def state_provider_from_string(dataarg,dataopts = None):
    dataopts = dataopts or {}
    if len(dataarg.split(':',1)) == 1:
        dataarg = 'local:'+dataarg
    for k in providersetup_handlers.keys():
        if dataarg.startswith(k):
            return providersetup_handlers[k](dataarg,dataopts)
    raise RuntimeError('unknown data type %s %s' % (dataarg, dataopts))
