import uuid
import os
import re
from exporters.writers.base_writer import BaseWriter
from retrying import retry

DROPBOX_RE = re.compile(r'dropbox://(.*)')


class DropboxWriter(BaseWriter):
    """
    Writes items for dropbox delivery.

    Needed parameters:

        - API_KEY (str)
            The app's api key.

        - API_SECRET (str)
            The app's secret key.

        - access_token (str)
            Client access token.

        - filebase (str)
            Base path to store the items in.
    """

    parameters = {
        'api_key': {'type': basestring, "default": "c6guvy3lez7xfxm"},
        'secret_key': {'type': basestring, "default": "ohiy5xgp01j6p2x"},
        'access_token': {'type': basestring},
        'aws_login': {'type': basestring},
        'aws_key': {'type': basestring},
        'filebase': {'type': basestring}
    }

    def __init__(self, options):
        import dropbox
        super(DropboxWriter, self).__init__(options)
        self.api_key = self.read_option('api_key')
        self.secret_key = self.read_option('secret_key')
        self.access_token = self.read_option('access_token')
        self.filebase = self.read_option('filebase').format(datetime.datetime.now())
        self.logger.info('DropboxWriter has been initiated. Sending to: {}'.format(self.filebase))
        self.writer_finished = False

    @retry(wait_exponential_multiplier=1000,
           wait_exponential_max=30000,
           stop_max_attempt_number=10)
    def _upload_chunk(self, uploader):
        chunk_size = 10 * 2 ** 20
        try:
            uploader.upload_chunked(chunk_size=chunk_size)
        except Exception as e:
            self.logger.warn('Error while uploading chunk: %s' % e.message)
            raise

    def _upload(self, target_path, file_to_upload):
        self.logger.info('Starting upload to: %s' % target_path)

        size = os.stat(file_to_upload).st_size
        with open(file_to_upload) as f:
            uploader = self.client.get_chunked_uploader(f, size)

            while uploader.offset < size:
                self._upload_chunk(uploader)

        response = uploader.finish(target_path, overwrite=True)
        self.logger.info("uploaded {size} to {path} with revision: {revision}".format(**response))

    def flush_callback(self, writer, filename):
        self._flush(writer.tempfile, filename)

    def _flush(self, file_to_upload, filename):
        targetpath = self.genfilename(filename)
        self._upload(targetpath, file_to_upload)

    def _close(self, complete=None, success_file=False):
        return

    @retry(stop_max_attempt_number=3)
    def s3copy(self, srckey, dst):
        self.logger.info('Downloading %s to temp file...' % srckey.name)
        with open(self._s3_tempfile, 'w') as fp:
            srckey.get_contents_to_file(fp)

        self._flush(self._s3_tempfile, dst)

        self._last_transfered_key = srckey

    def get_resume_position(self, extract_key):
        if not self._last_transfered_key:
            error = ('There is no support for resuming from remote dir'
                     'yet, resuming will fail!')
            self.logger.error(error)
            return
        return extract_key(self._last_transfered_key.name)

    def write(self, dump_path, group_key=None):
        self.client = dropbox.client.DropboxClient(self.access_token)
        self.filepath = DROPBOX_RE.match(self.filebase).group(1)
        try:
            self.client.file_create_folder(self.filepath)
        except dropbox.rest.ErrorResponse as e:
            self.logger.info(e.error_msg)
