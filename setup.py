import os
from setuptools import setup, find_packages

deps = [
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
  ]

setup(
  name = 'yadage',
  version = '0.19.4',
  description = 'yadage - YAML based adage',
  url = '',
  author = 'Lukas Heinrich',
  author_email = 'lukas.heinrich@cern.ch',
  packages = find_packages(),
  include_package_data = True,
  install_requires = deps,
  extras_require = {
    'celery' : [
       'celery',
       'redis'
    ],
    'viz': [
        #manually adding extras of adage[extra] because of pip
        #issue https://github.com/pypa/pip/issues/3189
        'pydot2',
        'pygraphviz',
        'pydotplus'
    ],
    'develop': [
       'pyflakes',
       'isort',
       'pytest>=3.2.0',
       'pytest-cov>=2.5.1',
       'python-coveralls'
    ]
  },
  entry_points = {
      'console_scripts': [
          'yadage-run=yadage.steering:main',
          'yadage-manual=yadage.manualcli:mancli',
          'yadage-util=yadage.utilcli:utilcli',
      ],
  },
  dependency_links = [
  ]
)
