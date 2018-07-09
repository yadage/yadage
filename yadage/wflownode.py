import datetime
import os
import time

import jsonpointer

import adage.node
import packtivity
import yadage.tasks as tasks


class YadageNode(adage.node.Node):
    '''
    Node object for yadage that extends the default with
    the ability to have prepublished results
    '''

    def __init__(self, name, task, identifier=None):
        super(YadageNode, self).__init__(name, task, identifier)

    def __repr__(self):
        lifetime = datetime.timedelta(seconds = (time.time() - self.define_time))
        runtime = None
        if self.state != adage.nodestate.DEFINED:
            referencetime = time.time() if not self.ready() else self.ready_by_time
            runtime = datetime.timedelta(seconds = (referencetime - self.submit_time))
        return '<YadageNode {} {} lifetime: {}  runtime: {} (id: {}) has result: {}>'.format(
            self.name, self.state, lifetime, runtime, self.identifier, self.has_result()
        )

    def has_result(self):
        if 'YADAGE_IGNORE_PREPUBLISHING' in os.environ:
            return self.successful()
        return (self.expected_result is not None) or self.successful()

    @property
    def expected_result(self):
        if 'YADAGE_IGNORE_PREPUBLISHING' in os.environ:
            return
        try:
            return self._prepublished_cache
        except AttributeError:
            self._prepublished_cache = packtivity.prepublish_default(self.task.spec, self.task.parameters.json(), self.task.state)
            return self._prepublished_cache

    @property
    def result(self):
        if self.expected_result is not None and 'YADAGE_IGNORE_PREPUBLISHING' not in os.environ:
            if self.ready() and self.successful():
                sanity =  super(YadageNode, self).result.json() == self.expected_result.json()
                if not sanity:
                    raise RuntimeError('prepublished and actual result differ:\n result:\n{}\n prepub:{}'.format(
                        super(YadageNode, self).result.json(),self.expected_result.json())
                )
            return self.expected_result
        return super(YadageNode, self).result

    def readfromresult(self,pointerpath, trackinputs = None, failsilently = False):
        if not self.has_result():
            if failsilently: return None
            raise RuntimeError('attempt')
        pointer = jsonpointer.JsonPointer(pointerpath)
        if trackinputs is not None:
            trackinputs.append(tasks.outputReference(self.identifier,pointer))
        v = self.result.resolve_ref(pointer)
        return v

    @classmethod
    def fromJSON(cls, data, state_deserializer):
        if data['task']['type'] == 'packtivity_task':
            task = tasks.packtivity_task.fromJSON(data['task'], state_deserializer)
            return cls(data['name'], task, data['id'])
        else:
            raise RuntimeError('unknown task type',data['task']['type'])
