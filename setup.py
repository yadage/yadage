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
    version="0.20.2",
    description="yadage - YAML based adage",
    url="https://github.com/yadage/yadage",
    author="Lukas Heinrich",
    author_email="lukas.heinrich@cern.ch",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.6",
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
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Physics",
        "Operating System :: OS Independent",
    ],
)
