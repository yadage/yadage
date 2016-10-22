Introduction
============

Requirements
------------

Beyond the pure python dependencies automatically installed by `pip`, you need the following additional libraries/software:
  * graphviz + development libraries

The best way to install these is via your favorite packages manager (`yum`, `apt`, `homebrew`). Please open an GitHub issue if we have forgotten any external dependencies.

Installation
------------

Yadage is available from PyPI and can be installed via `pip`::

  pip install yadage


Using the Yadage Docker Image
-----------------------------

One can use `yadage` without installation via the offical image. The docker image is currently published on the `Docker Hub`_) under::

  lukasheinrich/yadage:<release>

.. _`Docker Hub`: https://hub.docker.com/r/lukasheinrich/yadage/

It is built using the Dockerfile at the root of the source repository.

First Run
---------

To test the installation run this command::

  yadage-run -t from-github/testing/local-helloworld workdir workflow.yml -p par=World
