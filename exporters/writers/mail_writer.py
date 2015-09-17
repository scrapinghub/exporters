import email
import uuid
import boto
from exporters.writers.base_writer import BaseWriter
from retrying import retry

# credentials for sending email through Amazon SES
SES_LOGIN = ('AKIAID6WTWATZMQUKHWQ', 'KhTJzJGoqIK+F3CUZYsIdXeUAgGgjwGlIGqBS15i')


class MailWriter(BaseWriter):
    """
    Writes items for email delivery.

    Needed parameters:

        - tmp_folder (str)
            Path to store temp files.

        - email (str)
            Email address where data will be sent.

        - max_size (str)
            maximum size of the attachment for delivery.

        - max_sent (str)
            maximum amount of emails that would be sent.
    """

    parameters = {
        'email': {'type': basestring},
        'max_size': {'type': basestring, 'default': '10'},
        'max_sent': {'type': basestring, 'default': '5'}
    }

    def __init__(self, options, settings):
        super(MailWriter, self).__init__(options, settings)
        self.email = self.read_option('email')
        self.max_size = self.read_option('size')
        self.max_sent = self.read_option('sent')
        self.ses = boto.connect_ses(SES_LOGIN[0], SES_LOGIN[1])
        self.items_limit = 10
        self.logger.info('MailWriter has been initiated. Sending to: {}'.format(self.email))

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def write(self, dump_path, group_key=None):
        m = email.mime.multipart.MIMEMultipart()
        m['Subject'] = 'Test'
        m['From'] = 'Scrapinghub data services <dataservices@scrapinghub.com>'
        m['To'] = self.email

        # Message body
        part = email.mime.text.MIMEText('test file attached')
        m.attach(part)

        # Attachment
        key_name = '{}_{}.{}'.format('ds_dump', uuid.uuid4(), 'gz')
        part = email.mime.text.MIMEText('contents of test file here')
        part.add_header('Content-Disposition', 'attachment; filename=%s' % key_name)
        m.attach(part)

        self.ses.send_raw_email(source=m['From'], raw_message=m.as_string(), destinations=m['To'])

        self.logger.debug('Saved {}'.format(dump_path))
