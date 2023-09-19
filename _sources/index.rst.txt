.. Yadage documentation master file, created by
   sphinx-quickstart on Sat Oct 15 23:33:06 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Yadage - Declarative Workflow Spec and Engine
==================================

Contents:

.. toctree::
   :maxdepth: 2

   introduction
   definingworkflows
   executingworkflows
   contributing
   coderef

Overview
========

Why Yadage?
-----------

Yadage is two things:

  1. a set of JSON schemas to describe parametrized workflows based on dynamic directed acyclic graphs (DAGs) of tasks encapsulated in pre-packaged environments (e.g. using linux containers / Docker)
  2. an execution engine for such workflow definitions supporting both local and distributed execution

While there are many workflow engines already, most either make it impossible or cumbersome to run workflows, in which the complete DAG is only known at runtime. In *yadage* the idea of dynamic DAGs is central to the system and workflows are defined as a set of instruction, *stages* how to build up the DAG during run-time.

Hello World Example
-------------------

This is a simple one-stage workflow with a single parameter::

  stages:
    - name: hello_world
      dependencies: [init]
      scheduler:
        scheduler_type: singlestep-stage
        parameters:
          parone: {stages: init, output: par, unwrap: true}
          outputfile: '{workdir}/hello_world.txt'
        step:
          process:
            process_type: 'string-interpolated-cmd'
            cmd: 'echo Hello {parone} | tee {outputfile}'
          publisher:
            publisher_type: 'frompar-pub'
            outputmap:
              outputfile: outputfile
          environment:
            environment_type: 'docker-encapsulated'
            image: busybox

Since this workflow is stored on GitHub, you can run it simply using this command line::

  yadage-run -t from-github/testing/busybox-helloworld workdir workflow.yml -p par=World


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
