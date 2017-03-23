Executing Workflows
==================================

The *yadage-run* Command
------------------------

The main command line tool in `yadage` is the `yadage-run` command. This command reads a workflow definition, sequentially builds the DAG from the stage
definitions when their predicates become true and submits the individual packtivities (graph nodes) to a backend (default: Multiprocessing Pool). After execution a number of visualizations are produced for convenience.

Main Usage
``````````

The obligatory arguments for `yadage-run` a writeable work directory and the workflow definition::

  yadage-run <workdir> <workflow def>

If the workflow takes input parameters they can be provided as a third argument in YAML format::

  yadage-run <workdir> <workflow def> <input parameters>

Execution Backends
------------------

Yadage supports a number of different backends used to execute the packtivities. The default is a multiprocessing pool openend at the machine used to execute the ``yadage-run`` command. For multi-machine distributed execution, currently a shared filesystem is needed. Possible distributed backends are a Celery cluster or IPython clusters. Custom backends can be implemented by implementing as described in the packtivity documentaion.

The backend choice is set via the ``-b/--backend`` command line option

Example:
````````

A basic example with of usage with an ipython cluster with 4 cores woud be::

  yadage-run -b ipcluster:4 <workdir> <workflow def>




Manual execution using *yadage-manual*
--------------------------------------
