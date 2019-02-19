import logging
import os
import shutil
from six import string_types
import zipfile
from packtivity.statecontexts import load_state
from packtivity.statecontexts.posixfs_context import LocalFSState

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen

log = logging.getLogger(__name__)



def prepare_workdir_from_archive(initdir, inputarchive):
    if os.path.exists(initdir):
        raise RuntimeError("initialization directory exists and input archive give. Can't have both")
    os.makedirs(initdir)
    localzipfile = '{}/.yadage_inputarchive.zip'.format(initdir)
    f = urlopen(inputarchive)
    with open(localzipfile,'wb') as lf:
        lf.write(f.read())
    with zipfile.ZipFile(localzipfile) as zf:
        zf.extractall(path=initdir)
    os.remove(localzipfile)


def _merge_states(lhs,rhs):
    return LocalFSState(lhs.readwrite + rhs.readwrite,lhs.readonly + rhs.readonly)

class LocalFSProvider(object):
    def __init__(self, *base_states, **kwargs):
        base_states = list(base_states)
        self.nest = kwargs.get('nest', True)
        self.ensure = kwargs.get('ensure')
        self.init_states = kwargs.get('init_states')
        self.sub_inits = kwargs.get('sub_init_states',{})

        first = base_states.pop()
        assert first

        self.base = first

        while base_states:
            next_state = base_states.pop()
            if not next_state:
                continue
            self.base = _merge_states(self.base,next_state)

    def new_provider(self,name, init_states = None):
        new_base_ro = self.base.readwrite + self.base.readonly
        new_base_rw = [os.path.join(self.base.readwrite[0],name)]

        sub_init    = self.sub_inits.get(name,{}).get('_data')
        sub_inits   = [sub_init] if sub_init else []
        init_states = init_states or []

        newsinits = {k:v for k,v in self.sub_inits.get(name,{}).items() if k!='_data'}
        return LocalFSProvider(LocalFSState(new_base_rw,new_base_ro),
                               nest = self.nest,
                               ensure = self.ensure,
                               init_states = init_states + sub_inits,
                               sub_init_states = newsinits)


    def new_state(self,name, dependencies, readonly = False):
        '''
        creates a new context from an existing context.

        if subdir is True it declares a new read-write nested under the old
        context's read-write and adds all read-write and read-only locations
        of the old context as read-only. This is recommended as it makes rolling
        back changes to the global state made in this context easy.

        else the same readwrite/readonly configuration as the parent context is used
        '''

        readwrites = []
        if not readonly:
            readwrites = ['{}/{}'.format(self.base.readwrite[0],name)] if self.nest else self.base.readwrite

        log.debug('new state is: rw: %s', readwrites)
        state_identifier = name.replace('/','_') # replace in case name is nested path
        newstate = LocalFSState(readwrite = readwrites, identifier = state_identifier, dependencies = dependencies)

        if not readonly and self.ensure:
            newstate.ensure()

        return newstate

    def json(self):
        def subinit_json(subinits):
            def handle(k,v):
                if k=='_data':
                    return v.json() if v else None
                return {kk:handle(kk,vv) for kk,vv in v.items()}
            return {k: handle(k,v) for k,v in subinits.items()}
        d = {
            'state_provider_type': 'localfs_provider',
            'base_state': self.base.json(),
            'init_states': [s.json() for s in self.init_states] if self.init_states else [],
            'nest': self.nest,
            'ensure': self.ensure,
            'sub_init_states': subinit_json(self.sub_inits)
        }
        return d

    @classmethod
    def fromJSON(cls,jsondata, deserialization_opts):
        def subinit_load(subinit_data):
            def handle(k,v):
                if k == '_data':
                    return load_state(v,deserialization_opts) if v else None
                return {kk:handle(kk,vv) for kk,vv in v.items()}
            return {k:handle(k,v) for k,v in subinit_data.items()}

        instance = cls(
            load_state(jsondata['base_state'],deserialization_opts),
            nest = jsondata['nest'],
            ensure = jsondata['ensure'],
            init_states = [load_state(x,deserialization_opts) for x in jsondata['init_states']],
            sub_init_states = subinit_load(jsondata['sub_init_states'])
        )
        return instance

def setup_provider(dataarg, dataopts):
    workdir   = dataarg.split(':',2)[1]
    read      = dataopts.get('read',None)
    nest      = dataopts.get('nest',True)
    ensure    = dataopts.get('ensure',True)
    overwrite = dataopts.get('overwrite',False)
    subinits  = dataopts.get('subinits',{})
    pathbase  = dataopts.get('pathbase')

    if overwrite and os.path.exists(workdir):
        shutil.rmtree(workdir)

    init_states = []


    initdir = os.path.join(pathbase or workdir,dataopts.get('initdir','init'))
    inputarchive = dataopts.get('inputarchive',None)
    if initdir:
        if inputarchive:
            prepare_workdir_from_archive(initdir, inputarchive)
        init_state = LocalFSState(readonly = [initdir])
        init_states.append(init_state)

    def handle_init_spec(pathbase,spec):
        subinits = {}
        for k,v in spec.items():
            if isinstance(v,string_types):
                subinits[k] = {'_data': LocalFSState(readonly = [os.path.join(pathbase,v)])}
            if isinstance(v,dict):
                subinits.setdefault(k,{})['_data'] = LocalFSState(readonly = [os.path.join(pathbase,v.pop('_data'))]) if '_data' in v else None
                for kk,vv in handle_init_spec(pathbase,v).items():
                    subinits.setdefault(k,{})[kk] = vv
        return subinits


    if subinits:
        subinits = handle_init_spec(pathbase or workdir,subinits)

    writable_state = LocalFSState([workdir])
    return LocalFSProvider(read,writable_state,
        ensure = ensure, nest = nest, init_states = init_states,
        sub_init_states = subinits
    )
