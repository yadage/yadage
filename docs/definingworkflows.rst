Defining Workflows
==================================

Introduction
------------

Basic Terminology
-----------------

Packtivities:
`````````````

The atomic unit of the workflow is a `packtivity` -- a packaged activity. It represents a single parametrized processing step. The parameters a passed as JSON documents and the processing step is executed using one of multiple backends. After processing the packtivity publishes JSON data that includes relevant data for further processing (e.g. referencing files that were created during the processing).

Stages:
```````

Instead of describing a specific graph of tasks, a yadage workflow definition consists of a collection of `stages` that describe how an existing graph should be extended with additional nodes and edges. Starting from an empty graph (0 nodes, 0 edges), it is built up sequentially through application of these stages.

A stage consists of two pieces

1. A stage body (i.e. its scheduler):

  This section describes the logic how to define new nodes and new edges to attach them to the existing graph. Currently yadage supports two stages, one defining a single node and defining multiple nodes, both of which add edges according to the the data accessed from upstream nodes.

2. A predicate (i.e. its dependencies):

  The predicate (also referred to as the stages' dependencies) is a description of when the stage body is ready to be applied. Currently yadage supports a single predicate that takes a number of `JSON Path`_ expressions. Each expression selects a number of stages. The dependency is considered satisfied when all packtivities associated to that stage (i.e. nodes) have a published result.

.. _`JSON Path`: http://goessner.net/articles/JsonPath/

Using JSON references
`````````````````````

Writing the entire workflow in a single file is both cumbersome and limits re-usability of individual components (e.g. for packtivities used in multiple workflows).

During loading each workflow spec is intepreted  with respect to a `toplevel` address. If the workflow contains `JSON references`_ they are resolved with respect to that toplevel URL.

.. _`JSON references`: https://tools.ietf.org/id/draft-pbryan-zyp-json-ref-03.html

Example
.........


Defining a Packtivity
---------------------


Defining a Stage
----------------

Composition using Subworkflows
------------------------------


Validating Workflows
--------------------


Dumping Workflow JSON
---------------------
