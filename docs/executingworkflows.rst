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
``````````````````

Manual execution using *yadage-manual*
--------------------------------------
