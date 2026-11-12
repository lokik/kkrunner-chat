"""Client setup shared by integration tests."""


def open_job(endpoint, attachment, *, start=True, **options):
    parts = endpoint.open_run_parts(attachment, **options)
    parts.configure_observation()
    job = parts.job
    if start:
        job.start()
    return job
