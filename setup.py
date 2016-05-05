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
    'adage>0.2.0',
    'packtivity>0.0.1',
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
    'cap-schemas'
  ],
  entry_points = {
      'console_scripts': [
          'yadage-run=yadage.steering:main',
          'yadage-validate=yadage.validator_workflow:main'
      ],
  },
  dependency_links = [
      'https://github.com/lukasheinrich/adage/tarball/master#egg=adage-0.3.0',
      'https://github.com/lukasheinrich/packtivity/tarball/master#egg=packtivity-0.0.2',
      'https://github.com/lukasheinrich/cap-schemas/tarball/master#egg=cap-schemas-0.0.1'
  ]
)
