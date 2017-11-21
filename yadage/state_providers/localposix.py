import os
import logging
log = logging.getLogger(__name__)

from packtivity.statecontexts import load_state
from packtivity.statecontexts.posixfs_context import LocalFSState
from ..utils import prepare_workdir_from_archive



def _merge_states(lhs,rhs):
    return LocalFSState(lhs.readwrite + rhs.readwrite,lhs.readonly + rhs.readonly)

class LocalFSProvider(object):
    def __init__(self, *base_states, **kwargs):
        base_states = list(base_states)
        self.nest = kwargs.get('nest', True)
        self.ensure = kwargs.get('ensure')
        self.init_states = kwargs.get('init_states')
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
        return LocalFSProvider(LocalFSState(new_base_rw,new_base_ro),
                               nest = self.nest,
                               ensure = self.ensure,
                               init_states = init_states or [])


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

        readonlies = []
        for d in dependencies or []:
            if d.readwrite:
                readonlies += d.readwrite # if dep has readwrite add those
            else:
                readonlies += d.readonly # else add the readonlies

        log.debug('new state is: rw: %s, ro: %s', readwrites, readonlies)
        state_identifier = name.replace('/','_') # replace in case name is nested path
        newstate = LocalFSState(readwrite = readwrites, readonly = readonlies, identifier = state_identifier, dependencies = dependencies)

        if not readonly and self.ensure:
            newstate.ensure()

        return newstate

    def json(self):
        return {
            'state_provider_type': 'localfs_provider',
            'base_state': self.base.json(),
            'init_states': [s.json() for s in self.init_states] if self.init_states else [],
            'nest': self.nest,
            'ensure': self.ensure
        }

    @classmethod
    def fromJSON(cls,jsondata, deserialization_opts):
        return cls(
            load_state(jsondata['base_state'],deserialization_opts),
            nest = jsondata['nest'],
            ensure = jsondata['ensure'],
            init_state = [load_state(x,deserialization_opts) for x in jsondata['init_states']]
        )

def setup_provider(dataarg, dataopts):
    workdir = dataarg.split(':',2)[1]
    read = dataopts.get('read',None)
    nest = dataopts.get('nest',True)
    ensure = dataopts.get('ensure',True)

    init_states = []

    initdir = os.path.join(workdir,dataopts.get('initdir','init'))
    inputarchive = dataopts.get('inputarchive',None)
    if initdir:
        if inputarchive:
            prepare_workdir_from_archive(initdir, inputarchive)
        init_state = LocalFSState(readonly = [initdir])
        init_states.append(init_state)
    writable_state = LocalFSState([workdir])
    return LocalFSProvider(read,writable_state, ensure = ensure, nest = nest, init_states = init_states)
