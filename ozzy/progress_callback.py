import time


def get_transmission_names(is_upload):
    if is_upload:
        return 'upload', 'sent'
    else:
        return 'download', 'downloaded'


def format_log_progress_mesg(speed, transmitted_bytes, total_transfer_time,
                             is_upload, total_size=None):
    transmission_type, transmitted = get_transmission_names(is_upload)
    eta_string = ''
    if total_size and speed:
        eta = (total_size-transmitted_bytes) / speed
        eta_string = '[ETA: {eta:.1f}s]'.format(eta=eta)
    template = ('Average {transmission_type} speed: {speed:.2f} bytes/sec '
                '(bytes {transmitted}: {transmitted_bytes} of {total_size},'
                ' {transmission_type} elapsed time: {transfer_time} sec) {eta_string}')
    return template.format(
        speed=speed,
        transmitted_bytes=transmitted_bytes,
        transfer_time=total_transfer_time,
        transmission_type=transmission_type,
        transmitted=transmitted,
        total_size=total_size or 'unknown',
        eta_string=eta_string,
    )


class BaseProgressCallback(object):
    """Base class for building progress log callbacks
    """
    def __init__(self, logger, log_interval=60):
        self.start_ts = time.time()
        self.lastlog_ts = self.start_ts
        self.log_interval = log_interval
        self.logger = logger

    def log_transfer_progress(self, transmitted_bytes, total_size=None, is_upload=False):
        now = time.time()

        total_transfer_time = now - self.start_ts
        speed = float(transmitted_bytes) / total_transfer_time

        reached_log_interval_limit = (now - self.lastlog_ts) > self.log_interval
        if reached_log_interval_limit:
            self.lastlog_ts = now
            self.logger.info(
                format_log_progress_mesg(speed, transmitted_bytes,
                                         total_transfer_time, is_upload, total_size))


class BotoUploadProgress(BaseProgressCallback):
    """Progress logging callback for boto"""
    def __call__(self, transmitted_bytes, total_size):
        self.log_transfer_progress(transmitted_bytes, total_size=total_size, is_upload=True)


class BotoDownloadProgress(BaseProgressCallback):
    """Progress logging callback for boto"""
    def __call__(self, transmitted_bytes, total_size):
        self.log_transfer_progress(transmitted_bytes, total_size=total_size)


class SftpUploadProgress(BaseProgressCallback):
    """Progress logging callback for Pysftp's put"""
    def __call__(self, transferred, total_size):
        self.log_transfer_progress(transferred, total_size=total_size, is_upload=True)


class SftpDownloadProgress(BaseProgressCallback):
    """Progress logging callback for Pysftp's put"""
    def __call__(self, transferred, total_size):
        self.log_transfer_progress(transferred, total_size=total_size)


class FtpUploadProgress(BaseProgressCallback):
    """Progress logging callback for ftplib"""
    def __init__(self, *args, **kwargs):
        super(FtpUploadProgress, self).__init__(*args, **kwargs)
        self.transmitted_bytes = 0

    def __call__(self, block):
        self.transmitted_bytes += len(block)
        self.log_transfer_progress(self.transmitted_bytes, is_upload=True)


class FtpDownloadProgress(BaseProgressCallback):
    """Progress logging callback for ftplib"""
    def __init__(self, *args, **kwargs):
        super(FtpDownloadProgress, self).__init__(*args, **kwargs)
        self.transmitted_bytes = 0

    def __call__(self, block):
        self.transmitted_bytes += len(block)
        self.log_transfer_progress(self.transmitted_bytes)
