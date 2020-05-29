import pytest
import yadage.handlers.expression_handlers as exh
from yadage.utils import process_refs
from yadage.controllers import frommodel_controller


def test_stage_output_selector(nested_mapreduce_wflow):
    wflow = nested_mapreduce_wflow
    inputdata = {
        "here": [1, 2, 3],
        "deeply": {"nested": {"this": ["h", "e", "r", "e"]}},
        "nested_list": [[1, 2, 3]],
    }
    view = wflow.view()
    view.init({"input": inputdata})
    view.getRule(name="init").apply(wflow)
    frommodel_controller("", {}, wflow).sync_backend()

    result = exh.handlers["stage-output-selector"](
        view, {"stages": "init", "output": "input.here"}
    )
    values = process_refs(result, wflow.dag)
    assert values == [inputdata["here"]]

    result = exh.handlers["stage-output-selector"](
        view, {"stages": "init", "output": "input.here", "unwrap": True}
    )
    values = process_refs(result, wflow.dag)
    assert values == inputdata["here"]

    result = exh.handlers["stage-output-selector"](
        view, {"stages": "init", "output": "input.deeply", "unwrap": True}
    )
    values = process_refs(result, wflow.dag)
    assert values == inputdata["deeply"]

    result = exh.handlers["stage-output-selector"](
        view,
        {
            "stages": "init",
            "output": "input.nested_list",
            "unwrap": True,
            "flatten": True,
        },
    )
    values = process_refs(result, wflow.dag)
    assert values == [1, 2, 3]

    with pytest.raises(RuntimeError):
        result = exh.handlers["stage-output-selector"](
            view, {"stages": "init", "output": "nonexist"}
        )
