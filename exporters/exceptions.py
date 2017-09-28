from __future__ import absolute_import
import six
class InvalidExpression(Exception):
    pass


class WriterNotSupportingGroupedBatch(Exception):
    pass


class ConfigurationError(ValueError):
    "Configuration provided isn't valid."


class InvalidDateRangeError(ValueError):
    "Empty or impossible date range"


class UnsupportedCompressionFormat(ValueError):
    "Unsupported compression format."


class ConfigCheckError(ConfigurationError):
    def __init__(self, message="Configuration provided isn't valid.", errors={}):
        super(ConfigCheckError, self).__init__(message)
        self.errors = errors

    def __str__(self):
        if not self.errors:
            return self.message
        error_messages = []
        for section, errors in six.iteritems(self.errors):
            if isinstance(errors, six.string_types):
                error_messages.append('{}: {}'.format(section, errors))
            else:
                section_errors = '\n'.join(
                    '  {}: {}'.format(field, error) for field, error in six.iteritems(errors))
                error_messages.append('{}:\n{}'.format(section, section_errors))
        return '{}\n{}'.format(self.message, '\n'.join(error_messages))
