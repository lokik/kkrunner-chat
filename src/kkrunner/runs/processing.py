"""Result-processing synchronization contract.

Processing is coordinated by Run and durably claimed by RunManager.
"""


class ProcessingPending(RuntimeError):
    """Execution finished, but this observer does not own result processing."""


def captured_outcome(files, outcome):
    """Expose only paths belonging to this calculation's captured artifacts.

    Both a fresh Run and recovered postprocessing use the same provenance
    rules. A skipped remote output must not resolve to an older local file.
    """
    import os
    from kkrunner.files.availability import file_is_current

    if not file_is_current(files, outcome.input_file, files.input_versions):
        outcome.input_file = None
    captured_outputs = {os.path.abspath(v.path) for v in files.output_versions}
    for name in ("output_file", "error_file"):
        path = getattr(outcome, name)
        if path is not None and os.path.abspath(path) not in captured_outputs:
            setattr(outcome, name, None)
    return outcome
