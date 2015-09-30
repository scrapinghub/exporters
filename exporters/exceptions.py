class InvalidExpression(Exception):
    pass


class WriterNotSupportingGroupedBatch(Exception):
    pass


class ConfigurationError(ValueError):
    "Configuration provided isn't valid."
