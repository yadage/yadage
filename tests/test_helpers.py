import packtivity.utils
import yadage.utils
import os


def test_leafit():
    data = {
        "hello": "world",
        "list": [1, 2, 3],
        "nested": {"dict": ["like", "structure"]},
    }
    assert len(list(packtivity.utils.leaf_iterator(data))) == 6

    assert set((x.path, y) for x, y in packtivity.utils.leaf_iterator(data)) == {
        ("/hello", "world"),
        ("/list/0", 1),
        ("/list/1", 2),
        ("/list/2", 3),
        ("/nested/dict/0", "like"),
        ("/nested/dict/1", "structure"),
    }


def test_getinit(tmpdir):
    tmpdir.join("input.yml").write("input: [1,2,3]\n")

    data = yadage.utils.getinit_data(
        [str(tmpdir.join("input.yml"))], ["another=parameter"]
    )
    assert {"another": "parameter", "input": [1, 2, 3]} == data
