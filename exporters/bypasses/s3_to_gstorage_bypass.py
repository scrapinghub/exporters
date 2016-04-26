import datetime
from exporters.export_managers.base_bypass import RequisitesNotMet
from .base_s3_bypass import BaseS3Bypass
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials


S3_URL_EXPIRES_IN = 1800  # half an hour should be enough


class S3GStorageBypass(BaseS3Bypass):
    """
    Bypass executed by default when data source is an S3 bucket and data destination
    is a Google Storage bucket.
    It should be transparent to user. Conditions are:

        - S3Reader and GStorageWriter are used on configuration.
        - No filter modules are set up.
        - No transform module is set up.
        - No grouper module is set up.
        - GStorageWriter has not a items_limit set in configuration.
        - GStorageWriter has default items_per_buffer_write and size_per_buffer_write per default.
        - GStorageWriter filebase is the root of target bucket
    """

    def __init__(self, config, metadata):
        super(S3GStorageBypass, self).__init__(config, metadata)

        credentials = self.read_option('writer', 'credentials')
        self.project_id = credentials['project_id']
        self.gstorage_bucket = self.read_option('writer', 'bucket')
        gcredentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials)
        self.transfer_client = discovery.build('storagetransfer', 'v1', credentials=gcredentials)

    @classmethod
    def meets_conditions(cls, config):
        if not config.writer_options['name'].endswith('.GStorageWriter'):
            raise RequisitesNotMet
        filebase = config.writer_options['options']['filebase']
        if filebase not in ('', '/'):
            raise RequisitesNotMet
        super(S3GStorageBypass, cls).meets_conditions(config)

    def _copy_keys(self, source_bucket, keys):
        today = datetime.date.today()
        today_obj = {
            'day': today.day,
            'month': today.month,
            'year': today.year
        }
        transfer_job = {
            'description': "Exporters bypass job",
            'status': 'ENABLED',
            'projectId': self.project_id,
            'schedule': {
                # If scheduleEndDate is the same as scheduleStartDate, the
                # transfer will be executed only once
                'scheduleStartDate': today_obj,
                'scheduleEndDate': today_obj,
            },
            'transferSpec': {
                'objectConditions': {
                    'includePrefixes': [keys]
                },
                'awsS3DataSource': {
                    'bucketName': source_bucket.name,
                    'awsAccessKey': {
                        'accessKeyId': self.bypass_state.aws_key,
                        'secretAccessKey': self.bypass_state.aws_secret,
                    }
                },
                'gcsDataSink': {
                    'bucketName': self.gstorage_bucket
                }
            }
        }
        self.transfer_client.transferJobs().create(body=transfer_job).execute()
