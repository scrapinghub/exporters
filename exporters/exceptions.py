class InvalidExpression(Exception):
    "Invalid one-liner expression"


class WriterNotSupportingGroupedBatch(Exception):
    pass


class ConfigurationError(ValueError):
    "Configuration provided isn't valid"
