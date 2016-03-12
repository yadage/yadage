from setuptools import setup, find_packages

setup(
  name = 'yadage',
  version = '0.0.1',
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
    'click',
    'psutil',
    'requests[security]',
    'pyyaml',
    'jsonref',
    'jsonschema'
  ],
  entry_points = {
      'console_scripts': [
          'yadage-run=yadage.steering:main',
          'yadage-validate=yadage.validator_workflow:main'
      ],
  },
  dependency_links = [
  ]
)
