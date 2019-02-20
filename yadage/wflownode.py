import os
# import datetime
# import time

import jsonpointer

import adage.node
import adage.serialize

from packtivity import datamodel

from packtivity.backendutils import load_proxy
from .tasks import outputReference, packtivity_task

class YadageNode(adage.node.Node):
    '''
    Node object for yadage that extends the default with
    the ability to have prepublished results
    '''

    def __init__(self, name, task, identifier=None, result = None):
        super(YadageNode, self).__init__(name, task, identifier, result = result)
        self.expected_result = None

    def __repr__(self):
        # lifetime = datetime.timedelta(seconds = (time.time() - self.define_time))
        # runtime = None
        if self.state != adage.nodestate.DEFINED:
            pass
            # referencetime = time.time() if not self.ready() else self.ready_by_time
            # runtime = datetime.timedelta(seconds = (referencetime - self.submit_time))
        return '<{}/{}:{}|{}|{}>'.format(
            self.task.metadata.get('wflow_offset','-'),
            self.task.metadata.get('wflow_stage','-'),
            self.task.metadata.get('wflow_stage_node_idx','-'),
            str(self.state).lower(),
            'known' if self.has_result() else 'unknown'
        )

    def has_result(self):
        return (self.expected_result is not None) or self.successful()

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
            trackinputs.append(outputReference(self.identifier,pointer))
        v = self.result.resolve_ref(pointer)
        return v

    def json(self):
        json_or_nil = lambda x: None if x is None else x.json()
        d = adage.serialize.node_to_json(self,json_or_nil,json_or_nil)
        d['result'] = json_or_nil(self.result if self.has_result() else None)
        return d

    @classmethod
    def fromJSON(cls, data, deserialization_opts = None):
        if data['task']['type'] == 'packtivity_task':
            task   = packtivity_task.fromJSON(data['task'], deserialization_opts)
            result = datamodel.create(data['result'],getattr(task.state,'datamodel',{})) if data['result'] else None
            instance = cls(data['name'], task, data['id'],result)

            adage.serialize.set_generic_data(instance,data)
            instance.resultproxy = load_proxy(data['proxy'],deserialization_opts, best_effort_backend = False) if data['proxy'] else None
            return instance
        else:
            raise RuntimeError('unknown task type',data['task']['type'])
