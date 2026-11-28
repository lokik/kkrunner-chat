"""Client setup shared by integration tests."""


def open_job(endpoint, attachment, **options):
    parts = endpoint.open_run_parts(attachment, **options)
    parts.configure_observation()
    return parts.job
