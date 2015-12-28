import time


def get_transmission_names(is_upload):
    if is_upload:
        return 'upload', 'sent'
    else:
        return 'download', 'downloaded'


def render_simple_template(speed, transmitted_bytes, total_transfer_time, is_upload):

    transmission_type, transmitted = get_transmission_names(is_upload)
    template = ('Average {transmission_type} speed: {speed:.2f} bytes/sec '
                '(total bytes {transmitted}: {transmitted_bytes:.2f},'
                ' total {transmission_type} time: {transfer_time:.2f} sec)')
    return template.format(
        speed=speed,
        transmitted_bytes=transmitted_bytes,
        transfer_time=total_transfer_time,
        transmission_type=transmission_type,
        transmitted=transmitted,
    )


def render_eta_template(speed, transmitted_bytes, total_transfer_time, is_upload, total_size):
    transmission_type, transmitted = get_transmission_names(is_upload)
    eta = (total_size-transmitted_bytes) / speed
    template = ('Average {transmission_type} speed: {speed:.2f} bytes/sec '
                '(total bytes {transmitted}: {transmitted_bytes:.2f} of {total_size:.2f},'
                ' total {transmission_type} time: {transfer_time:.2f} sec) [ETA: {eta:.1f}s]')
    return template.format(
        speed=speed,
        transmitted_bytes=transmitted_bytes,
        transfer_time=total_transfer_time,
        transmission_type=transmission_type,
        transmitted=transmitted,
        total_size=total_size,
        eta=eta,
    )


def format_log_progress_mesg(speed, transmitted_bytes, total_transfer_time, is_upload, total_size):
    if total_size:
        return render_eta_template(speed, transmitted_bytes, total_transfer_time, is_upload, total_size)
    else:
        return render_simple_template(speed, transmitted_bytes, total_transfer_time, is_upload)


class BaseProgressCallback(object):
    """Base class for building progress log callbacks
    """
    def __init__(self, logger, log_interval=60, is_upload=False):
        self.start_ts = time.time()
        self.is_upload = is_upload
        self.lastlog_ts = self.start_ts
        self.log_interval = log_interval
        self.logger = logger

    def log_transfer_progress(self, transmitted_bytes, total_size=None):
        now = time.time()

        total_transfer_time = now - self.start_ts
        speed = float(transmitted_bytes) / total_transfer_time

        reached_log_interval_limit = (now - self.lastlog_ts) > self.log_interval
        if reached_log_interval_limit:
            self.lastlog_ts = now
            self.logger.info(format_log_progress_mesg(speed, transmitted_bytes,
                                              total_transfer_time, self.is_upload, total_size))


class BotoProgress(BaseProgressCallback):
    """Progress logging callback for boto"""
    def __call__(self, transmitted_bytes, total_size):
        self.log_transfer_progress(transmitted_bytes, total_size=total_size)


class SftpProgress(BaseProgressCallback):
    """Progress logging callback for Pysftp's put"""
    def __call__(self, transferred, total_size, _):
        self.log_transfer_progress(transferred, total_size=total_size)


class FtpProgress(BaseProgressCallback):
    """Progress logging callback for ftplib"""
    def __init__(self, *args, **kwargs):
        super(FtpProgress, self).__init__(*args, **kwargs)
        self.transmitted_bytes = 0

    def __call__(self, block):
        self.transmitted_bytes += len(block)
        self.log_transfer_progress(self.transmitted_bytes)
