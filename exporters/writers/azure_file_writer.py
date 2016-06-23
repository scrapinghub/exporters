import logging


logging.warning('Exporters naming has been deprecated. Please use ozzy instead')


from ozzy.writers.azure_file_writer import AzureFileWriter  # NOQA
