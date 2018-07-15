import json


def snapshot(workflow, wflowfile):
    '''
    snapshots the state of the workflow into two files:
        - a file holding the pure state of the workflow
        - a file holding the backend state, i.e. results to specific tasks
    '''
    with open(wflowfile, 'w') as wfile:
        json.dump(workflow.json(), wfile)
