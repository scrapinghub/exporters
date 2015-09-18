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

        - email (str)
            Email address where data will be sent.

        - subject (str)
            Subject of the email.

        - from (str)
            Sender of the email.

        - max_mails_sent (str)
            maximum amount of emails that would be sent.
    """

    parameters = {
        'email': {'type': list},
        'subject': {'type': basestring},
        'from': {'type': basestring},
        'max_mails_sent': {'type': int, 'default': 5},
        'aws_login': {'type': basestring},
        'aws_key': {'type': basestring}
    }

    def __init__(self, options):
        super(MailWriter, self).__init__(options)
        self.email = self.read_option('email')
        self.subject = self.read_option('subject')
        self.sender = self.read_option('from')
        self.max_mails_sent = self.read_option('max_mails_sent')
        self.mails_sent = 0
        self.items_per_buffer_write = self.options.get('items_per_buffer_write', ITEMS_PER_BUFFER_WRITE)
        self.ses = boto.connect_ses(self.options['aws_login'], self.options['aws_key'])
        self.logger.info('MailWriter has been initiated. Sending to: {}'.format(self.email))

    def write(self, dump_path, group_key=None):
        if self.max_mails_sent == self.mails_sent:
            raise ItemsLimitReached('Finishing job after items_limit reached: {} items written.'
                                    .format(self.mails_sent))
        m = email.mime.multipart.MIMEMultipart()
        m['Subject'] = self.subject
        m['From'] = self.sender

        # Attachment
        key_name = '{}_{}.{}'.format('ds_dump', uuid.uuid4(), 'gz')
        with open(dump_path, 'rb') as fd:
            part = email.mime.base.MIMEBase('application', 'octet-stream')
            part.set_payload(fd.read())
            email.encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=key_name)
            m.attach(part)

        for destination in self.email:
            self.send_mail(m, destination)
        self.mails_sent += 1
        self.logger.debug('Sent {}'.format(dump_path))

    @retry(wait_exponential_multiplier=500, wait_exponential_max=10000, stop_max_attempt_number=10)
    def send_mail(self, m, destination):
        self.ses.send_raw_email(source=m['From'], raw_message=m.as_string(), destinations=destination)
        self.logger.info('Sending email. Sending to: {}'.format(destination))
