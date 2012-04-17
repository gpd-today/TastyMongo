from __future__ import print_function
from __future__ import unicode_literals

from pyramid.response import Response


class TastyException(Exception):
    """A base exception for other tastypie-related errors."""

    def __init__( self, message='', error_code=0, *args, **kwargs ):
        super( TastyException, self ).__init__( message, *args, **kwargs )
        self.error_code = error_code


class ConfigurationError( TastyException ):
    pass


class ApiFieldError(ConfigurationError):
    """
    Raised when there is a configuration error with a ``ApiField``.
    """
    pass

class NotRegistered(ConfigurationError):
    """
    Raised when the requested resource isn't registered with the ``Api`` class.
    """
    pass


class NotFound(TastyException):
    """
    Raised when the resource/object in question can't be found.
    """
    pass


class HydrationError(TastyException):
    """Raised when there is an error hydrating data."""
    pass


class BadRequest(TastyException):
    """
    A generalized exception for indicating incorrect request parameters.

    Handled specially in that the message tossed by this exception will be
    presented to the end user.
    """
    pass


class ValidationError( TastyException ):
    pass


class UnsupportedFormat(BadRequest):
    """
    Raised when an unsupported serialization format is requested.
    """
    pass


class InvalidFilterError(BadRequest):
    """
    Raised when the end user attempts to use a filter that has not be
    explicitly allowed.
    """
    pass


class InvalidSortError(BadRequest):
    """
    Raised when the end user attempts to sort on a field that has not be
    explicitly allowed.
    """
    pass


class ImmediateHttpResponse(TastyException):
    """
    This exception is used to interrupt the flow of processing to immediately
    return a custom HttpResponse.

    Common uses include::

        * for authentication (like digest/OAuth)
        * for throttling

    """
    response = Response( body='No description provided.' )

    def __init__( self, response, *args, **kwargs ):
        super( ImmediateHttpResponse, self ).__init__(*args, **kwargs)
        self.response = response

    def __unicode__( self ):
        return unicode( self.response )
