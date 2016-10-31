class StaticProxy(object):
    def __init__(self,taskid):
        self.taskid = taskid

class StaticBackend(object):
    def __init__(self,resultdata):
        self.resultdata = resultdata

    def submit(self,task):
        raise RuntimeError('this backend is not meant for submission but just for loading a workflow')

    def result(self,resultproxy):
        return self.resultdata[resultproxy.taskid]

    def ready(self,resultproxy):
        return True

    def successful(self,resultproxy):
        raise NotImplementedError

    def fail_info(self,resultproxy):
        return 'cannot give reason :('
