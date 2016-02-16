import email
import uuid
import os
from exporters.writers.base_writer import BaseWriter
from exporters.default_retries import retry_short


class MaxMailsSent(Exception):
    """
    This exception is thrown when we try to send more than allowed mails number
    """


class MailWriter(BaseWriter):
    """
    Send emails with items files attached

        - email (str)
            Email address where data will be sent

        - subject (str)
            Subject of the email

        - from (str)
            Sender of the email

        - max_mails_sent (str)
            maximum amount of emails that will be sent
    """

    supported_options = {
        'emails': {'type': list},
        'subject': {'type': basestring},
        'from': {'type': basestring},
        'max_mails_sent': {'type': int, 'default': 5},
        'access_key': {
            'type': basestring,
            'env_fallback': 'EXPORTERS_MAIL_AWS_ACCESS_KEY',
        },
        'secret_key': {
            'type': basestring,
            'env_fallback': 'EXPORTERS_MAIL_AWS_SECRET_KEY',
        }
    }

    def __init__(self, options):
        import boto
        super(MailWriter, self).__init__(options)
        self.emails = self.read_option('emails')
        self.subject = self.read_option('subject')
        self.sender = self.read_option('from')
        self.max_mails_sent = self.read_option('max_mails_sent')
        self.mails_sent = 0
        self.ses = boto.connect_ses(self.options['access_key'], self.options['secret_key'])
        self.logger.info('MailWriter has been initiated. Sending to: {}'.format(self.emails))
        self.writer_finished = False

    def _write_mail(self, dump_path, group_key):
        if self.max_mails_sent == self.mails_sent:
            raise MaxMailsSent('Finishing job after max_mails_sent reached: {} mails sent.'
                               .format(self.mails_sent))

        m = email.mime.multipart.MIMEMultipart()
        m['Subject'] = self.subject
        m['From'] = self.sender

        # Attachment
        key_name = '{}_{}.{}'.format('ds_dump', uuid.uuid4(), 'gz')
        filesize = os.path.getsize(dump_path)
        with open(dump_path, 'rb') as fd:
            part = email.mime.base.MIMEBase('application', 'octet-stream')
            part.set_payload(fd.read())
            email.encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=key_name)
            m.attach(part)

        # Message body
        body = "File Name: {key_name} \n"
        body += "Size: {filesize}\n"
        body += "Number of Records: {buffered_items}\n"

        body = body.format(
            key_name=key_name,
            filesize=filesize,
            buffered_items=self.grouping_info[tuple(group_key)]['buffered_items']
        )
        part = email.mime.text.MIMEText(body)
        m.attach(part)

        for destination in self.emails:
            self.send_mail(m, destination)
        self.mails_sent += 1
        self.logger.debug('Sent {}'.format(dump_path))

    def write(self, dump_path, group_key=None):
        if self.writer_metadata['items_count']:
            self._write_mail(dump_path, group_key)
        else:
            self.logger.debug('Mail not sent. 0 records exported')

    @retry_short
    def send_mail(self, m, destination):
        self.logger.info('Sending email. Sending to: {}'.format(destination))
        self.ses.send_raw_email(source=m['From'], raw_message=m.as_string(), destinations=destination)
        self.logger.info('Email sent to {}'.format(destination))
