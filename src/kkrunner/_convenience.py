"""Simple execution using a newly configured client."""


def run(
    argv, *, target=None, backend=None, directory=".", input_file=None, output_file=None,
    stdin=None, error_file=None, input_files=(), output_files=(), cache_files=(),
    mpi_argv=None, mpi=None, staging=None, mpi_uses_stdin=True,
    persistent=None, detached=None, process_result=None, output_listener=None,
):
    """Run a command and wait; use Run.create for a separately managed lifetime."""
    from kkrunner.client.execution_client import ExecutionClient

    return ExecutionClient(backend, target=target).run(
        argv, directory=directory, input_file=input_file, output_file=output_file,
        stdin=stdin, error_file=error_file, input_files=input_files,
        output_files=output_files, cache_files=cache_files, mpi_argv=mpi_argv,
        mpi=mpi, staging=staging, mpi_uses_stdin=mpi_uses_stdin,
        persistent=persistent, detached=detached,
        process_result=process_result, output_listener=output_listener,
    )


def submit(
    argv, *, target=None, backend=None, directory=".", input_file=None, output_file=None,
    stdin=None, error_file=None, input_files=(), output_files=(), cache_files=(),
    mpi_argv=None, mpi=None, staging=None, mpi_uses_stdin=True,
    persistent=None, detached=None, process_result=None, output_listener=None,
):
    """Submit a command and return its Run without waiting for completion."""
    from kkrunner.client.execution_client import ExecutionClient

    return ExecutionClient(backend, target=target).submit(
        argv, directory=directory, input_file=input_file, output_file=output_file,
        stdin=stdin, error_file=error_file, input_files=input_files,
        output_files=output_files, cache_files=cache_files, mpi_argv=mpi_argv,
        mpi=mpi, staging=staging, mpi_uses_stdin=mpi_uses_stdin,
        persistent=persistent, detached=detached,
        process_result=process_result, output_listener=output_listener,
    )
