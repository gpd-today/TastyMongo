from __future__ import print_function
from __future__ import unicode_literals

from pyramid.response import Response
from pyramid.settings import asbool

from . import http
from .exceptions import NotRegistered, ConfigurationError, NotFound
from .serializers import Serializer
from .utils import *
from .resource import Resource


class Api( object ):
    """
    Implements a registry to tie together the various resources that make up
    an API.

    Especially useful for navigation, HATEOAS and for providing multiple
    versions of your API.

    Optionally supplying ``api_name`` allows you to name the API. Generally,
    this is done with version numbers (i.e. ``v1``, ``v2``, etc.) but can
    be named any string.
    """
    def __init__(self, config, api_name='api', api_version='v1' ):
        self.api_name = api_name
        self.api_version = api_version
        self._registry = {}

        if asbool( config.registry.settings.get( 'tastymongo.enable_CORS', 'false' ) ):
            self.enable_CORS = True
            self.CORS_settings = {
                'origin': str( config.registry.settings.get( 'tastymongo.CORS_origin', 'None' ) ),
                'headers': str( config.registry.settings.get( 'tastymongo.CORS_headers', 'None' ) ),
                'credentials': str( config.registry.settings.get( 'tastymongo.CORS_credentials', 'false' ) ),
            }
        else:
            self.enable_CORS = False

        # Parse `pyramid.debug_api` setting
        debug_api = asbool( config.registry.settings.get( 'pyramid.debug_api', 'false' ) )
        config.registry.settings[ 'pyramid.debug_api' ] = debug_api
        self.config = config

        self.route = '/{}/{}'.format( self.api_name, self.api_version )

        self.config.add_route( self.route, self.route + '/' )
        self.config.add_view( self.wrap_view( self, self.top_level ), route_name=self.route )

    @staticmethod
    def _handle_server_error( resource, request, exception ):
        data = {
            'code': getattr( exception, 'code', 0 ),
            'message': unicode( exception )
        }

        if request.registry.settings[ 'pyramid.debug_api' ]:
            import sys, traceback
            data[ 'traceback' ] = '\n'.join( traceback.format_exception( *( sys.exc_info() ) ) )

        if isinstance( resource, Resource ):
            desired_format = resource.determine_format( request )
            serialized = resource.serialize( request, data, desired_format )
        elif isinstance( resource, Api ):
            serializer = Serializer()
            desired_format = determine_format( request, serializer )
            serialized = serializer.serialize( data, format=desired_format )
        else:
            raise TypeError( "Argument 'resource' should be an instance of Api or Resource" )

        response_class = http.HTTPInternalServerError
        if isinstance( exception, NotFound ):
            response_class = http.HTTPNotFound

        return response_class( body=serialized, content_type=str( desired_format ), charset=b'UTF-8' )

    def wrap_view( self, resource, view ):
        """
        Wraps methods so they can be called in a more functional way as well
        as handling exceptions better.

        Note that if ``BadRequest`` or an exception with a ``response`` attr
        are seen, there is special handling to either present a message back
        to the user or return the response traveling with the exception.
        """
        def wrapper( request, *args, **kwargs ):
            try:
                if hasattr( view, '__call__' ):
                    callback = view
                else:
                    callback = getattr( resource, view )

                response = callback( request, *args, **kwargs )

                if request.is_xhr:
                    # IE excessively caches XMLHttpRequests, so we're disabling
                    # the browser cache here.
                    # See http://www.enhanceie.com/ie/bugs.asp for details.
                    response.cache_control = 'no-cache'

                if isinstance( response, basestring ):
                    response = Response( body=response )
            except Exception as e:
                # Return a raw error
                if hasattr(e, 'response'):
                    response = e.response
                else:
                    # Return a serialized error message.
                    response = Api._handle_server_error( resource, request, e )

            if self.enable_CORS:
                response = self.add_CORS_headers( request, response, resource, view )
            return response

        return wrapper

    def add_CORS_headers( self, request, response, resource, view ):

        if isinstance( resource, Api ):
            allowed = ('get', 'options')
        elif 'list' in str( view.im_func ):
            allowed = resource._meta.list_allowed_methods
        elif 'single' in str( view.im_func ):
            allowed = resource._meta.single_allowed_methods
        else:
            allowed = resource._meta.allowed_methods

        allowed = str( ','.join( map( unicode.upper, allowed ) ) )

        if self.CORS_settings[ 'origin' ] == '*':
            response.headers[ b'Access-Control-Allow-Origin' ] = request.headers.environ.get( 'HTTP_ORIGIN', b'*' )
        else:
            response.headers[ b'Access-Control-Allow-Origin' ] = self.CORS_settings[ 'origin' ]
        response.headers[ b'Access-Control-Allow-Headers' ] = self.CORS_settings[ 'headers' ]
        response.headers[ b'Access-Control-Expose-Headers' ] = self.CORS_settings[ 'headers' ]
        response.headers[ b'Access-Control-Allow-Credentials' ] = self.CORS_settings[ 'credentials' ]
        response.headers[ b'Access-Control-Allow-Methods' ] = str( allowed )
        response.headers[ b'Allow' ] = str( allowed )

        return response

    def register( self, resource ):
        """
        Registers an instance of a ``Resource`` subclass with the API.

        @type resource: Resource
        """
        resource_name = getattr( resource._meta, 'resource_name', None )

        # Also add a hook to the Api on the resource
        resource._meta.api = self
        resource.__class__.Meta.api = self

        if resource_name is None:
            raise ConfigurationError( "Resource='{}' must define a 'resource_name'.".format( resource ) )

        self._registry[ resource_name ] = resource

        # add 'schema' action
        schema_name = self.build_route_name( resource_name, 'schema' )
        self.config.add_route( schema_name, '{}/{}/schema/'.format( self.route, resource_name ) )
        self.config.add_view( self.wrap_view( resource, resource.get_schema ), route_name=schema_name )

        # add 'list' action
        list_name = self.build_route_name( resource_name, 'list' )
        self.config.add_route( list_name, '{}/{}/'.format( self.route, resource_name ) )
        self.config.add_view( self.wrap_view( resource, resource.dispatch_list ), route_name=list_name )

        # add 'single' action
        single_name = self.build_route_name( resource_name, 'single' )
        self.config.add_route( single_name, '{}/{}/{{id}}/'.format( self.route, resource_name ) )
        self.config.add_view( self.wrap_view( resource, resource.dispatch_single ), route_name=single_name )

    def unregister(self, resource_name):
        """
        If present, unregisters a resource from the API.
        """
        if resource_name in self._registry:
            del(self._registry[resource_name])
        else:
            raise NotRegistered( "No resource was registered for resource_name='{}'.".format( resource_name ) )

    def resource_by_name( self, name ):
        return self._registry.get( name, None )

    def resource_for_class( self, cls ):
        '''
        @param cls: either a resource or document class
        '''
        for resource in self._registry.values():
            if isinstance( resource, cls ) or resource._meta.object_class and resource._meta.object_class == cls:
                return resource

        raise ValueError( 'Could not find matching resource for class={}'.format( cls ) )

    def resource_for_document( self, document ):
        # This becomes non-deterministic if there's more than a single Resource for a certain Document class.
        # We may need to set introduce a canonical resource.
        for resource in self._registry.values():
            if resource._meta.object_class and isinstance( document, resource._meta.object_class ):
                return resource

        raise ValueError( 'Could not find matching resource for document={}'.format( document ) )

    def resource_for_collection( self, collection ):
        # This becomes non-deterministic if there's more than a single Resource for a certain collection.
        # We may need to set introduce a canonical resource.
        for resource in self._registry.values():
            if resource._meta.object_class and resource._meta.object_class._meta['collection'] == collection:
                return resource

        raise ValueError( 'Could not find matching resource for collection={}'.format( collection ) )

    def resource_for_uri( self, uri ):
        resource_name = uri.split( '/' )[ -3 ]
        if resource_name in self._registry:
            return self._registry[ resource_name ]

        raise ValueError( 'Could not find matching resource for uri={}'.format( uri ) )
    
    def build_route_name(self, resource_name, operation):
        if resource_name is not None:
            route_name = '{}/{}/{}/'.format(self.route, resource_name, operation)
        else:
            route_name = '{}/{}/'.format(self.route, operation)

        return route_name

    def get_id_from_resource_uri( self, value ):
        if isinstance( value, basestring ) and value.startswith( self.route ):
            # '/api/v1/<resource_name>/<objectid>/' or some other string
            parts = value.split( '/' )
            if len( parts ) == 6:
                return parts[-2]

        return None

    def build_uri( self, request, id=None, resource_name=None, operation=None, route_name=None, absolute=False ):
        if route_name is None:
            route_name = self.build_route_name( resource_name, operation )

        if absolute:
            return request.route_url( route_name, id=id)
        else:
            return request.route_path( route_name, id=id)

    def top_level( self, request ):
        """
        A view that returns a serialized list of all resources registered
        to the ``Api``. Useful for discovery.
        """
        serializer = Serializer()
        available_resources = {}

        for resource_name in sorted(self._registry.keys()):
            available_resources[resource_name] = {
                'list_endpoint': self.build_uri( request, resource_name=resource_name, operation='list' ),
                'schema': self.build_uri( request, resource_name=resource_name, operation='schema' ),
            }

        desired_format = determine_format(request, serializer)

        serialized = serializer.serialize( available_resources, format=desired_format )
        return Response( body=serialized, content_type=str( desired_format ), charset=b'UTF-8' )

