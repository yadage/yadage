import logging
from adage.pollingexec import yes_man

from .interactive import custom_decider, decide_step, interactive_deciders
from .utils import advance_coroutine
from .handlers.utils import handler_decorator

log = logging.getLogger(__name__)

handlers, strategy = handler_decorator()


def get_strategy(strategystring, strategyopts=None):
    strategyopts = strategyopts or {}
    for k in handlers.keys():
        if strategystring.startswith(k):
            return handlers[k](strategystring, strategyopts)

    raise RuntimeError("Unknown Strategy %s", strategystring, handlers.keys())


@strategy("interactive")
def interactive_strategy(name, opts):
    extend, submit = interactive_deciders()
    return dict(extend_decider=extend, submit_decider=submit)


@strategy("askforsubmit")
def askforsubmit(name, opts=None):
    idbased = opts.get("idbased", False)
    extend_decider = yes_man()
    advance_coroutine(extend_decider)

    submit_decider = custom_decider(decide_step, idbased=idbased)()
    advance_coroutine(submit_decider)

    return dict(extend_decider=extend_decider, submit_decider=submit_decider)


@strategy("target")
def target(name, opts=None):
    opts = opts or {}
    idbased = opts.get("idbased", False)
    _, targetname = name.split(":", 1)
    targetoffset, targetname = targetname.rsplit("/", 1)

    def isfinished(controller, idbased):
        dag = controller.adageobj.dag
        targetnode = dag.getNodeByName(
            targetname,
            nodefilter=lambda x: x.task.metadata["wflow_offset"] == targetoffset,
        )
        if targetnode:
            return (targetnode.ready(), targetnode.successful())
        return False, False

    def upstream(dag, target_identifier):
        predecessors = [*dag.predecessors(target_identifier)]
        return list(
            set(predecessors + [g for p in predecessors for g in upstream(dag, p)])
        )

    def submit_if_node_is_upstream(node, controller, idbased):
        dag = controller.adageobj.dag
        if idbased:
            node = dag.getNode(node)
        matched_nodes = controller.adageobj.view(targetoffset).getSteps(targetname)
        assert len(matched_nodes) <= 1
        target_node = matched_nodes[0] if matched_nodes else None
        node_identifier = node.identifier
        log.debug(
            "decide if submitting node: %s %s %s", node, node.identifier, target_node
        )
        if target_node:
            target_identifier = target_node.identifier
            target_upstream = upstream(dag, target_identifier)
            if node_identifier == target_identifier:
                return True
            if not node_identifier in target_upstream:
                log.debug("not submitting %s since it is not in upstream of target")
                return False
        return True

    extend_decider = yes_man()
    advance_coroutine(extend_decider)

    submit_decider = custom_decider(submit_if_node_is_upstream, idbased=idbased)()
    advance_coroutine(submit_decider)

    finish_decider = custom_decider(isfinished, idbased=idbased, unroll_data=False)()
    advance_coroutine(finish_decider)

    return dict(
        extend_decider=extend_decider,
        submit_decider=submit_decider,
        finish_decider=finish_decider,
    )
