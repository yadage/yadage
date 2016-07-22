import yadage.yadagestep
import adage.backends
from celery import shared_task
from celery.result import AsyncResult
from packtivity import packtivity
from packtivitybackend import AdagePacktivityBackendBase, PacktivityProxyBase

@shared_task
def packtivity_task(spec, attributes, context):
    '''celery enabled packtiity'''
    print 'running a packtivity via a celery task'
    return packtivity(spec,attributes,context)

@shared_task
def init_task():
    '''dummy task'''
    print 'running a dummytask used for init steps'

class PacktivityCeleryProxy(PacktivityProxyBase):
    def proxyname(self):
        return 'PacktivityCeleryProxy'

    def details(self):
        return {
            'task_id':self.proxy.task_id,
            'task_name':self.proxy.task_name
        }

    @classmethod
    def fromJSON(cls,data):
        proxy = AsyncResult(
            data['proxydetails']['task_id'],
            task_name = data['proxydetails']['task_name']
        )
        return cls(proxy)


class PacktivityCeleryBackend(AdagePacktivityBackendBase):
    def __init__(self,app):
        super(PacktivityCeleryBackend,self).__init__(adage.backends.CeleryBackend(app))

    def submit(self,task):
        '''
        if the task type is a genuine yadagestep we submit is as a packtivity callable,
        if it's an init step, which has a trivial call body, we'll pass a dummy task
        '''
        tasktype = type(task)
        if tasktype == yadage.yadagestep.yadagestep:
            resultproxy = packtivity_task.delay(task.spec,task.attributes,task.context)
        elif tasktype == yadage.yadagestep.initstep:
            resultproxy = init_task.delay()
        else:
            raise RuntimeError('cannot figure out how to submit a task of type {}'.format(tasktype))

        return PacktivityCeleryProxy(resultproxy)
