from setuptools import setup, find_packages

setup(
  name = 'yadage',
  version = '0.7.3',
  description = 'yadage - YAML based adage',
  url = '',
  author = 'Lukas Heinrich',
  author_email = 'lukas.heinrich@cern.ch',
  packages = find_packages(),
  include_package_data = True,
  install_requires = [
    'functools32',
    'adage',
    'packtivity',
    'cap-schemas',
    'click',
    'psutil',
    'requests[security]>=2.9',
    'pyyaml',
    'jsonref',
    'jsonschema',
    'jsonpointer>=1.10',
    'jsonpath_rw',
    'packtivity',
    'adage',
    'jq'
  ],
  extras_require = {
    'celery' : ['celery','redis']
  },
  entry_points = {
      'console_scripts': [
          'yadage-run=yadage.steering:main',
          'yadage-manual=yadage.manualcli:mancli',
          'yadage-validate=yadage.validator_workflow:main',
      ],
  },
  dependency_links = [
  ]
)
