"""Result-processing synchronization contract.

Processing is coordinated by Run and durably claimed by RunManager.
"""


class ProcessingPending(RuntimeError):
    """Execution finished, but this observer does not own result processing."""
