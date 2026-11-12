KKRunner
==========

KKRunner runs external programs locally, over SSH, or through a scheduler.
It owns execution, declared files and durable run history. Your application
owns input preparation, scientific interpretation and presentation.
The core library requires only Python and ``platformdirs``.

Start with :doc:`quickstart`. For restartable calculations, continue with
:doc:`persistent-runs`; use :doc:`projects` when runs belong to named projects.

.. toctree::
   :maxdepth: 2
   :caption: User guide

   installation
   quickstart
   persistent-runs
   projects
   files
   backends
   client
   remote
   command-line

.. toctree::
   :maxdepth: 2
   :caption: Reference and development

   architecture
   api
   development

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
