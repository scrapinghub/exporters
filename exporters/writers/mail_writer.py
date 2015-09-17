import email
import uuid
import boto
from exporters.writers.base_writer import BaseWriter
from exporters.writers.base_writer import ItemsLimitReached
from retrying import retry

ITEMS_PER_BUFFER_WRITE = 5000


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
        'max_sent': {'type': int, 'default': 5},
        'aws_login': {'type': basestring},
        'aws_key': {'type': basestring}
    }

    def __init__(self, options, settings):
        super(MailWriter, self).__init__(options, settings)
        self.email = self.read_option('email')
        self.max_sent = self.read_option('max_sent')
        self.mails_sent = 0
        self.items_per_buffer_write = self.options.get('items_per_buffer_write', ITEMS_PER_BUFFER_WRITE)
        self.ses = boto.connect_ses(self.options['aws_login'], self.options['aws_key'])
        self.items_limit = 10
        self.logger.info('MailWriter has been initiated. Sending to: {}'.format(self.email))

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def write(self, dump_path, group_key=None):
        if self.items_limit and self.max_sent == self.mails_sent:
            raise ItemsLimitReached('Finishing job after items_limit reached: {} items written.'
                                    .format(self.mails_sent))

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
        self.mails_sent += 1
        self.logger.debug('Saved {}'.format(dump_path))
