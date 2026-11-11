"""Client setup shared by integration tests."""


def open_job(endpoint, attachment, *, start=True, **options):
    job = endpoint.open_run_parts(attachment, **options).job
    if start:
        job.start()
    return job
