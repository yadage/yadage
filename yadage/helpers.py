from backends.staticbackend import StaticProxy

def set_backend(dag,backend,proxymaker):
    '''
    sets backend and proxies for each node in the DAG.
    proxymaker is a 1-ary function that takes the node object and
    returns a suitable result proxy
    '''
    for nodename in dag.nodes():
        n = dag.getNode(nodename)
        n.backend = backend
        n.resultproxy = proxymaker(n)


def set_static_backend(dag,backend):
    '''
    sets the backend for the case of a static backend
    Proxies are set using the node identifier
    '''
    set_backend(
        dag,
        backend,
        proxymaker = lambda n: StaticProxy(n.identifier)
    )
