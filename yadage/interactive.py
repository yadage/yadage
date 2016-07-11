import click

def decide_rule(rule,state):
    click.secho('we could extend DAG with rule', fg = 'blue')
    click.secho('rule: {}/{} ({})'.format(rule.offset,rule.rule.name,rule.identifier))
    shall = raw_input(click.style("Shall we? (y/N) ", fg = 'blue')).lower() == 'y'
    if shall:
        click.secho('ok we will extend.', fg = 'green')
    else:
        click.secho('maybe another time...', fg = 'yellow')
    return shall

def decide_step(dag,nodeobj):
    click.echo('we could submit a DAG node (id: {}) DAG is: {}'.format(nodeobj,dag))
    shall = raw_input(click.style("Shall we? (y/N) ", fg = 'magenta')).lower() == 'y'
    if shall:
        click.secho('ok we will submit.', fg = 'green')
    else:
        click.secho('will not submit for now...', fg = 'yellow')
    return shall

def custom_decider(decide_func):
    # we yield until we receive some data via send()
    def decider():
        data = yield
        while True:
            data = yield decide_func(*data)
    return decider

def interactive_deciders():
    '''
    returns a tuple (extend,submit) of already-primed deciders for both
    extension and submission
    '''
    extend_decider = custom_decider(decide_rule)()
    extend_decider.next() #prime decider

    submit_decider = custom_decider(decide_step)()
    submit_decider.next() #prime decider

    return extend_decider,submit_decider
