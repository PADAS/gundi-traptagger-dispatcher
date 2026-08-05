class ConfigurationNotFound(Exception):
    pass


class ConfigurationValidationError(Exception):
    pass


class ReferenceDataError(Exception):
    pass


class DispatcherException(Exception):
    pass


class NonRetryableDispatchError(DispatcherException):
    """Delivery cannot succeed by retrying (e.g. permanent 4xx from the destination).
    The message must be sent to the dead-letter topic and acked."""

    pass


class TooManyRequests(Exception):
    pass
