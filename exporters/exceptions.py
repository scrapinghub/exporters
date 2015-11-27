class InvalidExpression(Exception):
    pass


class WriterNotSupportingGroupedBatch(Exception):
    pass


class ConfigurationError(ValueError):
    "Configuration provided isn't valid."


class ItemsLimitReached(Exception):
    """
    This exception is thrown when the desired items number has been reached
    """