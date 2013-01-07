from __future__ import print_function
from __future__ import unicode_literals

class Authentication( object ):
    """
    A simple base class to establish the protocol for auth.

    By default, this indicates the user is always authenticated.
    """
    def is_authenticated( self, request, **kwargs ):
        """
        Identifies if the user is authenticated to continue or not.

        Should return either ``True`` if allowed, ``False`` if not or an
        ``HttpResponse`` if you need something custom.
        """
        return not not request.user

    def get_identifier( self, request ):
        """
        Provides a unique string identifier for the requestor.

        This implementation returns a combination of IP address and hostname.
        """
        return "%s_%s" % ( request.remote_addr, request.host )


class NoAuthentication( Authentication ):
    def is_authenticated( self, request, **kwargs ):
        return True