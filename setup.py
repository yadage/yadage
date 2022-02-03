import os
from setuptools import setup, find_packages

deps = [
    "adage>=0.10.2",  # Ensure networkx>=2.4
    "packtivity",
    "yadage-schemas",
    "click",
    "psutil",
    "requests[security]>=2.9",
    "pyyaml",
    "jsonref",
    "jsonschema",
    "jsonpointer>=1.10",
    "jsonpath_rw",
    "checksumdir",
    "glob2",
    "jq",
]


if "READTHEDOCS" in os.environ:
    deps = [
        "yadage-schemas",
        "click",
        "psutil",
        "requests[security]>=2.9",
        "pyyaml",
        "jsonref",
        "jsonschema",
        "jsonpointer>=1.10",
        "jsonpath_rw",
        "checksumdir",
        "glob2",
    ]

setup(
    name="yadage",
    version="0.20.1",
    description="yadage - YAML based adage",
    url="",
    author="Lukas Heinrich",
    author_email="lukas.heinrich@cern.ch",
    packages=find_packages(),
    include_package_data=True,
    install_requires=deps,
    extras_require={
        "celery": ["celery", "redis"],
        "viz": ["adage[viz]>=0.10.3", "pydotplus>=2.0.0"],
        "lint": [
            "pyflakes",
            "isort",
            "black[jupyter]>=22.1.0",
        ],
        "develop": [
            "pre-commit",
            "pytest>=3.2.0",
            "pytest-cov>=2.5.1",
            "python-coveralls",
        ],
    },
    entry_points={
        "console_scripts": [
            "yadage-run=yadage.steering:main",
            "yadage-manual=yadage.manualcli:mancli",
            "yadage-util=yadage.utilcli:utilcli",
        ]
    },
    dependency_links=[],
)
