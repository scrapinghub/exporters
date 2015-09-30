class InvalidExpression(Exception):
    pass


class WriterNotSupportingGroupedBatch(Exception):
    pass


class OptionValueError(ValueError):
    "Invalid value provided to a module option"
