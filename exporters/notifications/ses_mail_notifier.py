import logging


logging.warning('Exporters naming has been deprecated. Please use ozzy instead')


from ozzy.notifications.ses_mail_notifier import SESMailNotifier  # NOQA
