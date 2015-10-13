incompatibility_rules = {
    'exporters.writers.mail_writer.MailWriter': [{
        'name': 'exporters.notifications.s3_mail_notifier.S3MailNotifier',
        'action': 'ignore'}],
    'exporters.export_formatter.csv_export_formatter.CSVExportFormatter': [{
        'name': 'exporters.writers.console_writer.ConsoleWriter',
        'action': 'fail'
    }]
}
