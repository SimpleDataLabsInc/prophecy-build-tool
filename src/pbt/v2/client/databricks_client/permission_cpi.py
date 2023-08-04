class PermissionsApi(object):
    def __init__(self, client):
        self.client = client

    def patch_job(self, scheduler_job_id:str, _data):
        return self.client.perform_query(
            'PATCH', f'/2.0/preview/permissions/jobs/{scheduler_job_id}', data=_data)
