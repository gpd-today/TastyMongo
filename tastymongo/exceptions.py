from __future__ import print_function
from __future__ import unicode_literals

from pyramid.response import Response

class TastyMongoError(Exception):
    """A base exception for other tastypie-related errors."""
    pass


class ObjectDoesNotExist(TastyMongoError):
    """Object not found."""
    pass

class ConfigurationError( TastyMongoError ):
    pass

class HydrationError(TastyMongoError):
    """Raised when there is an error hydrating data."""
    pass


class NotRegistered(TastyMongoError):
    """
    Raised when the requested resource isn't registered with the ``Api`` class.
    """
    pass


class NotFound(TastyMongoError):
    """
    Raised when the resource/object in question can't be found.
    """
    pass


class ApiFieldError(TastyMongoError):
    """
    Raised when there is a configuration error with a ``ApiField``.
    """
    pass


class UnsupportedFormat(TastyMongoError):
    """
    Raised when an unsupported serialization format is requested.
    """
    pass


class BadRequest(TastyMongoError):
    """
    A generalized exception for indicating incorrect request parameters.

    Handled specially in that the message tossed by this exception will be
    presented to the end user.
    """
    pass


class InvalidFilterError(BadRequest):
    """
    Raised when the end user attempts to use a filter that has not be
    explicitly allowed.
    """
    pass


class InvalidSortError(TastyMongoError):
    """
    Raised when the end user attempts to sort on a field that has not be
    explicitly allowed.
    """
    pass


class ImmediateHttpResponse(TastyMongoError):
    """
    This exception is used to interrupt the flow of processing to immediately
    return a custom HttpResponse.

    Common uses include::

        * for authentication (like digest/OAuth)
        * for throttling

    """
    response = Response( body='Nothing provided.' )

    def __init__(self, response):
        self.response = response