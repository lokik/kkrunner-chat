Installation
==============

KKRunner supports Python 3.9 and newer. The core depends on ``platformdirs`` and
``psutil``; scheduler support and documentation tools are optional extras.

For this development alpha, install from a KKRunner checkout or a supplied
wheel. From the root of the checkout:

.. code-block:: console

   python -m venv .venv
   . .venv/bin/activate
   python -m pip install .

This installs a copy of the package. If you are editing its source, use
``python -m pip install --editable .`` instead; changes to Python files then
apply without reinstalling. To install a supplied wheel, pass its pathname
to ``python -m pip install``.

For PSI/J scheduler backends, install the extra:

.. code-block:: console

   python -m pip install '.[psij]'

For documentation dependencies and the local HTML build, see
:ref:`build-documentation`.

For SSH execution, install the same KKRunner version on the target and make
``kkrunner-agent`` available to noninteractive SSH commands. An absolute
command can be supplied as ``("/opt/kkrunner/bin/kkrunner-agent",)``.
Scheduler execution needs the PSI/J extra and a configured scheduler on the
execution host. MPI execution needs a compatible MPI launcher there.

Platforms
-----------

Managed local execution and the direct-process remote service use POSIX process
groups and Unix sockets. CI covers Linux and macOS, including durable local
supervision and reconnect on Apple Silicon and Intel. Windows is not supported
by this supervisor. Raw local execution has no restart guarantee; an SSH client
needs the system ``ssh`` executable and a supported execution host.

Process identity uses the public ``psutil`` API. Linux creation times are
normalized against boot time to withstand system-clock changes. Other platforms
use the reported absolute creation time; clock adjustments and timestamp
resolution can limit identity reliability. Unverifiable identities yield
``UNKNOWN`` rather than assuming that a PID still belongs to the original run.
See :doc:`architecture` for the identity checks and their limits.

A client does not need ASE or scientific parsers even when the application that
submitted its runs uses them.
