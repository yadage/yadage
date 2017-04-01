import os
from setuptools import setup, find_packages

deps = [
    'functools32',
    'adage',
    'packtivity',
    'yadage-schemas',
    'click',
    'psutil',
    'requests[security]>=2.9',
    'pyyaml',
    'jsonref',
    'jsonschema',
    'jsonpointer>=1.10',
    'jsonpath_rw',
    'checksumdir',
    'glob2',
    'jq'
]


if 'READTHEDOCS' in os.environ:
  deps = [
    'functools32',
    'yadage-schemas',
    'click',
    'psutil',
    'requests[security]>=2.9',
    'pyyaml',
    'jsonref',
    'jsonschema',
    'jsonpointer>=1.10',
    'jsonpath_rw',
    # 'packtivity',
    # 'adage',
    'checksumdir',
    'glob2',
    # 'jq'
  ]

setup(
  name = 'yadage',
  version = '0.10.7',
  description = 'yadage - YAML based adage',
  url = '',
  author = 'Lukas Heinrich',
  author_email = 'lukas.heinrich@cern.ch',
  packages = find_packages(),
  include_package_data = True,
  install_requires = deps,
  extras_require = {
    'celery' : ['celery','redis']
  },
  entry_points = {
      'console_scripts': [
          'yadage-run=yadage.steering:main',
          'yadage-manual=yadage.manualcli:mancli',
          'yadage-validate=yadage.validator_workflow:main',
          'yadage-util=yadage.utilcli:utilcli',
      ],
  },
  dependency_links = [
  ]
)
