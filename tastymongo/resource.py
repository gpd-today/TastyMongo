from __future__ import print_function
from __future__ import unicode_literals

from . import fields
from . import http
from .serializers import Serializer
from .exceptions import *
from .constants import ALL, ALL_WITH_RELATIONS, QUERY_TERMS, LOOKUP_SEP
from .utils import determine_format
from .bundle import Bundle
from .authentication import Authentication
from .throttle import BaseThrottle
from .paginator import Paginator

from pyramid.response import Response
from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned, Q
from mongoengine.errors import ValidationError as MongoEngineValidationError

try:
    from mongoengine_relational.relationalmixin import RelationManagerMixin, set_difference
except ImportError, e:
    RelationManagerMixin = None

try:
    from mongoengine_privileges.privilegemixin import PrivilegeMixin
except ImportError, e:
    PrivilegeMixin = None

import mongoengine
import mongoengine.fields as mongofields
from mongoengine.document import Document
from bson import ObjectId, DBRef

from copy import copy
from operator import or_, and_
import collections 
from constants import *

from kitchen.text.converters import getwriter
import sys
UTF8Writer = getwriter('utf8')
sys.stdout = UTF8Writer(sys.stdout)


class ResourceOptions( object ):
    """
    A configuration class for `Resource`.

    Provides sane defaults and the logic needed to augment these settings with
    the internal `class Meta` used on `Resource` subclasses.
    """
    serializer = Serializer()
    authentication = Authentication()
    throttle = BaseThrottle()
    allowed_methods = [ 'get', 'post', 'put', 'delete', 'options' ]
    list_allowed_methods = None
    single_allowed_methods = None
    limit = 20
    max_limit = 1000
    api = None
    resource_name = None
    default_format = 'application/json'
    filtering = {}
    ordering = []
    paginator_class = Paginator
    object_class = None
    queryset = None
    fields = []
    excludes = []
    include_resource_uri = True
    use_absolute_uris = False
    return_data_on_post = True
    return_data_on_put = True

    def __new__( cls, meta=None ):
        overrides = {}

        # Handle overrides.
        if meta:
            for override_name in dir( meta ):
                # No internals please.
                if not override_name.startswith( '_' ):
                    overrides[override_name] = getattr( meta, override_name )

        allowed_methods = overrides.get( 'allowed_methods', ['get', 'post', 'put', 'delete', 'options'] )

        if overrides.get( 'list_allowed_methods', None ) is None:
            overrides['list_allowed_methods'] = allowed_methods

        if overrides.get( 'single_allowed_methods', None ) is None:
            overrides['single_allowed_methods'] = allowed_methods

        # We accept the meta.filtering as two types: a list of fields on which we allow all filter types, or a dict
        # of fields specifying each allowed filter type. If we get a list, change it to a dict here.
        if  isinstance( overrides.get( 'filtering', None ),  ( list, tuple, set ) ):
            filtering_dict = {}
            for field in overrides[ 'filtering' ]:
                filtering_dict[ field ] = ALL

                # check for related fields; their default is that relations can be filtered on as well
                if hasattr( meta, 'object_class' ) and hasattr( meta.object_class, '_fields' ):
                    if isinstance( meta.object_class._fields.get( field ), mongofields.ReferenceField ):
                        filtering_dict[ field ] = ALL_WITH_RELATIONS
                    # also check for to_many_fields, which are listfields of referencefields:
                    elif isinstance( meta.object_class._fields.get( field ), mongofields.ListField ) and \
                            isinstance( meta.object_class._fields[ field ].field, mongofields.ReferenceField ):
                        filtering_dict[ field ] = ALL_WITH_RELATIONS
            overrides[ 'filtering' ] = filtering_dict

        return object.__new__( type( str( 'ResourceOptions' ), ( cls, ), overrides ))


class DeclarativeMetaclass( type ):

    def __new__( cls, name, bases, attrs ):
        attrs['base_fields'] = {}
        declared_fields = {}

        # Inherit any fields from parent clas( ses ).
        try:
            parent_classes = [b for b in bases if issubclass( b, Resource )]
            # Simulate the MRO.
            parent_classes.reverse()

            for p in parent_classes:
                parent_class_fields = getattr( p, 'base_fields', {})

                for field_name, fld in parent_class_fields.items():
                    attrs['base_fields'][field_name] = copy( fld )
        except NameError:
            pass

        # Find fields explicitly set on the Resource
        for attr, obj in attrs.items():
            if isinstance( obj, fields.ApiField ):
                field = attrs.pop( attr )
                declared_fields[ attr ] = field

        # Add the explicitly defined fields to our base_fields
        attrs['base_fields'].update( declared_fields )
        attrs['declared_fields'] = declared_fields

        # Create the class
        new_class = super( DeclarativeMetaclass, cls ).__new__( cls, name, bases, attrs )

        # Create a new 'ResourceOptions' class based on the contents of a resource's 'Meta' class
        opts = getattr( new_class, 'Meta', None )
        new_class._meta = ResourceOptions( opts )

        if not getattr( new_class._meta, 'resource_name', None ):
            # No `resource_name` provided. Attempt to auto-name the resource.
            class_name = new_class.__name__
            name_bits = [bit for bit in class_name.split( 'Resource' ) if bit]
            resource_name = ''.join( name_bits ).lower()
            new_class._meta.resource_name = resource_name

        if getattr( new_class._meta, 'include_resource_uri', True ):
            if not 'resource_uri' in new_class.base_fields:
                new_class.base_fields['resource_uri'] = fields.StringField( readonly=True )
        elif 'resource_uri' in new_class.base_fields and not 'resource_uri' in attrs:
            del( new_class.base_fields['resource_uri'] )

        for field_name, fld in new_class.base_fields.items():
            if hasattr( fld, 'contribute_to_class' ):
                fld.contribute_to_class( new_class, field_name )

        return new_class


class Resource( object ):
    __metaclass__ = DeclarativeMetaclass

    def __init__( self, api=None ):
        self.fields = { k: copy( v ) for k, v in self.base_fields.items() }

        if api:
            self._meta.api = api

    def __getattr__( self, name ):
        if name in self.fields:
            return self.fields[name]
        raise AttributeError( name )

    def _prepare_request( self, request, type=None, method=None ):
        if not hasattr( request, 'api' ):
            request.api = {
                'errors': collections.defaultdict(list),
                'updated': set(),
                'saved': set(),
                'created': set(),
                'to_save': set(),
                'to_delete': set(),
                'deleted': set(),
                'type': type,
                'method': method,
                'resource': self
            }

        return request

    def get_resource_uri( self, request, data=None, absolute=False ):
        """
        This function should return the relative or absolute uri of the 
        bundle or object.
        """
        raise NotImplementedError()

    def dehydrate_resource_uri( self, bundle ):
        """
        For the automatically included `resource_uri` field, dehydrate
        the relative URI for the given bundle.
        """
        try:
            return self.get_resource_uri( bundle.request, bundle )
        except NotImplementedError:
            return '<not implemented>'

    def build_schema( self ):
        """
        Returns a dictionary of all the fields on the resource and some
        properties about those fields.

        Used by the `schema/` endpoint to describe what will be available.
        """
        data = {
            'fields': {},
            'default_format': self._meta.default_format,
            'allowed_list_http_methods': self._meta.list_allowed_methods,
            'allowed_single_http_methods': self._meta.single_allowed_methods,
            'default_limit': self._meta.limit,
        }

        if self._meta.ordering:
            data['ordering'] = self._meta.ordering

        if self._meta.filtering:
            data['filtering'] = self._meta.filtering

        for field_name, fld in self.fields.items():
            data['fields'][field_name] = {
                'default': fld.default,
                'type': fld.dehydrated_type,
                'required': fld.required,
                'readonly': fld.readonly,
                'help_text': fld.help_text,
                'unique': fld.unique,
            }
        return data

    def determine_format( self, request ):
        """
        Used to determine the desired format.

        Largely relies on `tastypie.utils.mime.determine_format` but here
        as a point of extension.
        """
        return determine_format( request, self._meta.serializer, default_format=self._meta.default_format )

    def check_method( self, request, allowed=None ):
        """
        Ensures that the HTTP method used on the request is allowed to be
        handled by the resource.
        
        Takes an `allowed` parameter, which should be a list of lowercase
        HTTP methods to check against. Usually, this looks like::

            # The most generic lookup.
            self.check_method( request, self._meta.allowed_methods )

            # A lookup against what's allowed for list-type methods.
            self.check_method( request, self._meta.list_allowed_methods )

            # A useful check when creating a new endpoint that only handles
            # GET.
            self.check_method( request, ['get'] )
        """
        if allowed is None:
            allowed = []

        request_method = request.method.lower()

        # Make `patch` an alias to `put`, the difference is theoretical only.
        if request_method == 'patch':
            request_method = 'put'

        if request_method == "options":
            allows = str( ','.join( map( unicode.upper, allowed ) ) )
            response = http.HTTPResponse( allows )
            response.headers[b'Allow'] = allows
            raise ImmediateHTTPResponse( response=response )

        if not request_method in allowed:
            allows = ','.join( map( unicode.upper, allowed ))
            response = http.HTTPMethodNotAllowed( body='Allowed methods={0}'.format( allows ))
            raise ImmediateHTTPResponse( response=response )

        return request_method

    @property
    def may_post(self):
        """
        Checks to ensure `post` is within `allowed_methods`.
        """
        allowed = set(self._meta.list_allowed_methods + self._meta.single_allowed_methods)
        return 'post' in allowed

    @property
    def may_put(self):
        """
        Checks to ensure `put` is within `allowed_methods`.

        Used when hydrating related data.
        """
        allowed = set(self._meta.list_allowed_methods + self._meta.single_allowed_methods)
        return 'put' in allowed

    @property
    def may_delete(self):
        """
        Checks to ensure `delete` is within `allowed_methods`.
        """
        allowed = set(self._meta.list_allowed_methods + self._meta.single_allowed_methods)
        return 'delete' in allowed

    def is_authenticated( self, request ):
        """
        Handles checking if the user is authenticated and dealing with
        unauthenticated users.

        Mostly a hook, this uses class assigned to `authentication` from
        `Resource._meta`.
        """
        # Authenticate the request as needed.
        auth_result = self._meta.authentication.is_authenticated( request )

        if isinstance( auth_result, Response ):
            raise ImmediateHTTPResponse( response=auth_result )

        if not auth_result is True:
            raise ImmediateHTTPResponse( response=http.HTTPUnauthorized() )

    def check_throttle( self, request ):
        """
        Handles checking if the user should be throttled.

        Mostly a hook, this uses class assigned to `throttle` from
        `Resource._meta`.
        """
        identifier = self._meta.authentication.get_identifier( request )

        # Check to see if they should be throttled.
        if self._meta.throttle.should_be_throttled( identifier, request ):
            # Throttle limit exceeded.
            raise ImmediateHTTPResponse( response=http.HTTPTooManyRequests() )

    def log_throttled_access( self, request ):
        """
        Handles the recording of the user's access for throttling purposes.

        Mostly a hook, this uses class assigned to `throttle` from
        `Resource._meta`.
        """
        self._meta.throttle.accessed( self._meta.authentication.get_identifier( request ), request )

    def create_response( self, bundles=None, request=None, data=None, response_class=Response, serializer_options=None, **kwargs ):
        '''
        Extracts the common "which-format/serialize/return-response" cycle.

        @param bundles
        @type bundles: Bundle or List<Bundle>
        @param data:
        @type data: object
        '''
        if request:
            desired_format = self.determine_format( request )
        else:
            desired_format = self._meta.default_format

        data = data or {}

        if bundles is not None:
            single_bundle = False
            if not isinstance( bundles, collections.Iterable ):
                single_bundle = True
                bundles = [ bundles ]

            bundles = self.pre_serialize( bundles, request )

            if single_bundle:
                data = bundles[ 0 ]
            else:
                data[ 'objects' ] = bundles

        serialized = self.serialize( request, data, desired_format, serializer_options )
        return response_class( body=serialized, content_type=str( desired_format ), charset=b'UTF-8', **kwargs )


    def deserialize( self, request, data, format=None ):
        """
        Analogous to python 'unpickle': translates serialized `data` in a given 
        `format` to python data structures.

        It relies on the request properly sending a `CONTENT_TYPE` header,
        falling back to the default format if not provided.

        Mostly a hook, this uses the `Serializer` from `Resource._meta`.
        """
        format = format or request.content_type or self._meta.default_format
        data = self._meta.serializer.deserialize( data, format )
        data = self.post_deserialize( data, request )
        return data

    def post_deserialize( self, data, request ):
        """
        A hook to alter data just after it has been received from the user &
        gets deserialized.

        Useful for altering the user data before any hydration is applied.
        """
        return data

    def build_bundle( self, request, obj=None, data=None ):
        """
        Given either an object, a data dictionary or both, builds a `Bundle`
        for use throughout the `dehydrate/hydrate` cycle.

        Given a dictionary-like structure is provided, a fresh bundle is 
        created using that data.

        If the data contains a resource_uri, any other keys in the data are 
        assumed to be updates to the existing object's properties.
        If the data contains no resource_uri and no object is provided, an 
        empty object from `Resource._meta.object_class` is created so that 
        attempts to access `bundle.obj` do not fail 
        (i.e. during validation/hydration)

        Errors are added to the bundle if a new resource may not be created or 
        if an existing resource is not found or may not be updated.
        """
        if isinstance( data, basestring ):
            # Assume data /is/ the uri
            data = { 'resource_uri': data }

        if data and 'resource_uri' in data:
            # Try to retrieve the object and put it in fresh bundle.
            obj = self.obj_get_single( request=request, uri=data['resource_uri'] )

        if obj is None:
            obj = self._meta.object_class()

        bundle = Bundle( obj=obj, data=data, request=request )
        if len( bundle.data ) > 1:
            bundle.uri_only = False

        return bundle

    def pre_hydrate( self, bundles, request ):
        '''
        A hook for allowing some custom hydration on the data specific to this
        resource before each field's hydrate function is called.
        '''
        return bundles

    def hydrate( self, bundles, request ):
        """
        Takes data from the resource and converts it to a form ready to be 
        stored on objects. Returns a fully hydrated bundle.

        Creates related bundles for related fields, instantiating corresponding
        objects along the way and nesting them in the data for the main bundle. 

        The result of the hydrate function is a fully populated bundle with 
        nested bundles for related objects. 

        Errors encountered along the way are propagated to the parent bundle.
        """
        single_bundle = False
        if not isinstance( bundles, collections.Iterable ):
            single_bundle = True
            bundles = [ bundles ]

        bundles = self.pre_hydrate( bundles, request )

        for bundle in bundles:
            for field_name, fld in self.fields.items():

                if ( request.method.lower() == 'patch' or request.method.lower() == 'put' ) and field_name not in bundle.data:
                    # When patching, ignore values not present in the data
                    continue

                # You may provide a custom method on the resource that will replace
                # the default hydration behaviour for the field.
                callback = getattr(self, "hydrate_{0}".format(field_name), None)
                if callable( callback ):
                    data = callback( bundle )
                elif fld.readonly:
                    continue
                else:
                    data = fld.hydrate( bundle )

                if getattr(fld, 'is_related', False):
                    if getattr(fld, 'is_tomany', False):

                        # ToManyFields return a list of bundles or an empty list.
                        setattr( bundle.obj, fld.attribute, [b.obj for b in data] )

                    else:
                        # ToOneFields return a single bundle or None.
                        if data is None:
                            setattr( bundle.obj, fld.attribute, None )
                        else:
                            setattr( bundle.obj, fld.attribute, data.obj )

                else:
                    # An ordinary field returns its converted data.
                    if fld.attribute:
                        setattr( bundle.obj, fld.attribute, data )

                # Reassign the -possibly changed- data
                bundle.data[ field_name ] = data

        if single_bundle:
            bundles = bundles[ 0 ]

        return bundles

    def save( self, bundle ):
        raise NotImplementedError()

    def dehydrate( self, bundles, request ):
        """
        Given a list of bundles with object instances, extract the information
        from them to populate the resource data.
        """
        single_bundle = False
        if not isinstance( bundles, collections.Iterable ):
            single_bundle = True
            bundles = [ bundles ]

        if not single_bundle and hasattr( self, '_prefetch_documents' ):
            self._prefetch_documents( bundles, request )

        for bundle in bundles:
            # Dehydrate each field.
            for field_name, fld in self.fields.items():
                bundle.data[field_name] = fld.dehydrate( bundle )

                # Check for an optional method to do further dehydration.
                method = getattr( self, "dehydrate_{0}".format( field_name ), None )
                if callable( method ):
                    bundle.data[field_name] = method( bundle )

        bundles = self.post_dehydrate( bundles, request )

        if single_bundle:
            bundles = bundles[0]

        return bundles

    def post_dehydrate( self, bundles, request ):
        '''
        A hook for allowing some custom dehydration on the whole resource after 
        each field's dehydrate function has been called.
        '''
        return bundles

    def pre_serialize( self, bundles, request ):
        """
        A hook to alter data just before it gets serialized & sent to the user.

        Useful for restructuring/renaming aspects of the what's going to be
        sent.
        """
        return bundles

    def serialize( self, request, data, format, options=None ):
        """
        Analogous to python 'pickle': translates python `data` to a given 
        output `format` suitable for transfer over the wire.

        Given a request, data and a desired format, produces a serialized
        version suitable for transfer over the wire.

        Mostly a hook, this uses the `Serializer` from `Resource._meta`.
        """
        return self._meta.serializer.serialize( data, format, options )


    def dispatch_list( self, request, **kwargs ):
        """
        A view for handling the various HTTP methods ( GET/POST/PUT/DELETE ) over
        the entire list of resources.
        
        Relies on `Resource.dispatch` for the heavy-lifting.
        """
        return self.dispatch( 'list', request, **kwargs )

    def dispatch_single( self, request, **kwargs ):
        """
        A view for handling the various HTTP methods ( GET/POST/PUT/DELETE ) on
        a single resource.

        Relies on `Resource.dispatch` for the heavy-lifting.
        """
        return self.dispatch( 'single', request, **kwargs )

    def dispatch( self, request_type, request, **kwargs ):
        """
        Handles the common operations ( allowed HTTP method, authentication,
        throttling, method lookup ) surrounding most CRUD interactions.
        """
        #import cProfile, pstats, StringIO
        #pr = cProfile.Profile()
        #pr.enable()

        allowed_methods = getattr( self._meta, '{0}_allowed_methods'.format(request_type), None )
        request_method = self.check_method( request, allowed=allowed_methods )

        self._prepare_request( request, request_type, request_method )

        # Determine which callback we're going to use
        method = getattr( self, '{0}_{1}'.format( request_method, request_type ), None )
        if not callable( method ):
            error = 'Method="{0}_{1}" is not implemented for resource="{2}"'.format( request_method, request_type, self._meta.resource_name )
            raise ImmediateHTTPResponse( response=http.HTTPNotImplemented( body=error ))

        self.is_authenticated( request )
        self.check_throttle( request )

        # All clear. Process the request.
        response = method( request, **kwargs )
        self.log_throttled_access(request)

        #pr.disable()
        #s = StringIO.StringIO()
        #sortby = 'cumulative'
        #ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        #ps.print_stats()
        #print( s.getvalue() )

        return response

    def get_schema( self, request ):
        """
        Returns a serialized form of the schema of the resource.

        Calls `build_schema` to generate the data. This method only responds
        to HTTP GET.

        Should return a HTTPResponse ( 200 OK ).
        """
        self.check_method( request, allowed=['get'] )
        self.is_authenticated( request )
        self.check_throttle( request )
        self.log_throttled_access(request)
        return self.create_response( data=self.build_schema(), request=request )

    def get_list( self, request ):
        """
        Returns a serialized list of resources.

        Calls `obj_get_list` to provide the objects to be dehydrated
        and serialized.

        Should return an HTTPResponse ( 200 OK ).
        """
        objects = self.obj_get_list( request=request, **request.matchdict )
        ordered_objects = self.apply_ordering( objects, options=request.GET.mixed() )

        paginator = self._meta.paginator_class(
            request.GET,
            ordered_objects,
            resource_uri=self.get_resource_uri( request ),
            limit=self._meta.limit,
            max_limit=self._meta.max_limit,
        )
        data = paginator.page()

        # Create a bundle for every object and dehydrate those bundles individually
        bundles = [ self.build_bundle( request=request, obj=obj ) for obj in data['objects'] ]
        bundles = self.dehydrate( bundles, request )

        return self.create_response( bundles, data=data, request=request )

    def get_single( self, request ):
        """
        Returns a single serialized resource.

        Calls `obj_get_single` to provide the object to be dehydrated
        and serialized.

        Should return an HTTPResponse ( 200 OK ).
        """
        try:
            obj = self.obj_get_single( request=request, **request.matchdict )
        except DoesNotExist, e:
            return http.HTTPNotFound()
        except MultipleObjectsReturned, e:
            return http.HTTPMultipleChoices( "More than one resource is found at this URI." )

        bundle = self.build_bundle( request=request, obj=obj )
        if bundle:
            bundle = self.dehydrate( bundle, request )

        return self.create_response( bundle, request )

    def post_list( self, request, **kwargs ):
        """
        Creates a new Resource.

        Returns `HTTPCreated` (201 Created) if all went well.
        Returns `HTTPBadRequest` (500) with any errors that occurred.
        """
        data = self.deserialize( request, request.body, format=request.content_type )

        bundle = self.build_bundle( request=request, data=data )
        bundle = self.hydrate( bundle, request )

        bundle = self.save( bundle )

        location = self.get_resource_uri( request, bundle )

        if self._meta.return_data_on_post:
            # Re-populate the data from the objects.
            bundle = self.dehydrate( bundle, request )
            return self.create_response( bundle, request, response_class=http.HTTPCreated, location=location )
        else:
            return http.HTTPCreated( location=location )
        
    def post_single(self, request, **kwargs):
        """
        Not implemented since we only allow posting to root resources, not
        self-referential subresources.
        """
        return http.HTTPNotImplemented('post_single is not possible')

    def put_list(self, request, **kwargs):
        """
        Updates the resources in the data. 
        Does not remove existing resources.

        Returns `HTTPAccepted` (204) if all went well, or `HTTPNoContent` (204)
        if return data was also requested.
        Returns `HTTPBadRequest` (500) with any errors that occurred.
        """
        data = self.deserialize( request, request.body, format=request.content_type )

        bundles = [ self.build_bundle( request=request, data=item ) for item in data ]
        bundles = self.hydrate( bundles, request )

        bundles = [ self.save( bundle ) for bundle in bundles ]

        if self._meta.return_data_on_put:
            # Re-populate the data from the objects.
            bundles = self.dehydrate( bundles, request )
            return self.create_response( bundles, request, response_class=http.HTTPAccepted )
        else:
            return http.HTTPNoContent()

    def put_single( self, request, **kwargs ):
        """
        Updates an existing Resource.

        Returns `HTTPAccepted` (204) if all went well, or `HTTPNoContent` (204)
        if return data was also requested.
        Returns `HTTPBadRequest` (500) with any errors that occurred.
        """
        data = self.deserialize( request, request.body, format=request.content_type )
        data[ 'resource_uri' ] = self.get_resource_uri( request, request.path )

        bundle = self.build_bundle( request=request, data=data )
        bundle = self.hydrate( bundle, request )
        bundle = self.save( bundle )

        location = self.get_resource_uri( request, bundle )

        if self._meta.return_data_on_put:
            # Re-populate the data from the objects.
            bundle = self.dehydrate( bundle, request )
            return self.create_response( bundle, request, response_class=http.HTTPAccepted, location=location )
        else:
            return http.HTTPNoContent( location=location )
        
    def delete_list(self, request, **kwargs):
        """
        Destroys a collection of resources/documents.

        Calls ``obj_delete_list``.

        If the resources are deleted, return ``HttpNoContent`` (204 No Content).
        """
        self.obj_delete_list(request=request, **self.remove_api_resource_names(kwargs))
        return http.HTTPNoContent()

    def delete_single(self, request, **kwargs):
        """
        Destroys a single resource/object.

        Calls `obj_delete_single`.

        If the resource is deleted, return `HTTPNoContent` (204 No Content).
        If the resource did not exist, return `HTTP404` (404 Not Found).
        """
        # construct a filter from the request path.
        kwargs['uri'] = request.path
        try:
            self.obj_delete_single(request=request, **kwargs)
            return http.HTTPNoContent()
        except NotFound:
            return http.HTTPNotFound()

    def obj_get_single( self, request, **kwargs ):
        """
        Fetches a single object at a given resource_uri.

        This needs to be implemented at the user level. 
        This should raise `NotFound` or `MultipleObjects` exceptions
        when there's no or multiple objects at the resource_uri.
        """
        raise NotImplementedError()

    def obj_get_list( self, request, **kwargs ):
        """
        Fetches a list of objects at a given resource_uri.

        This needs to be implemented at the user level.
        Returns an empty list if there are no objects.
        """
        raise NotImplementedError()

    def obj_delete_single(self, request, **kwargs):
        """
        Deletes a single object.

        This needs to be implemented at the user level.

        `DocumentResource` includes a working version for MongoEngine
        `Documents`.
        """
        raise NotImplementedError()

    def obj_delete_list(self, request, **kwargs):
        """
        Deletes an entire list of objects.

        `DocumentResource` includes a working version for MongoEngine
        `Documents`.
        """
        raise NotImplementedError()


class DocumentDeclarativeMetaclass( DeclarativeMetaclass ):
    # Subclassed to handle specifics for MongoEngine Documents
    def __new__( cls, name, bases, attrs ):
        meta = attrs.get( 'Meta' )

        # We may define a queryset or an object_class.
        if meta and hasattr( meta, 'queryset' ) and not hasattr( meta, 'object_class' ):
            setattr( meta, 'object_class', meta.queryset._document )

        new_class = super( DocumentDeclarativeMetaclass, cls ).__new__( cls, name, bases, attrs )
        include_fields = getattr( new_class._meta, 'fields', [] )
        excludes = getattr( new_class._meta, 'excludes', [] )

        for field_name, fld in new_class.base_fields.items():
            if field_name == 'resource_uri':
                # Embedded objects don't have their own resource_uri
                if meta and hasattr( meta, 'object_class' ) and issubclass( meta.object_class, mongoengine.EmbeddedDocument ):
                    del( new_class.base_fields[field_name] )
                continue
            if fld.attribute and hasattr( new_class._meta, 'object_class') and not hasattr( new_class._meta.object_class, fld.attribute):
                raise ConfigurationError( "Field `{0}` on `{1}` has an attribute `{2}` that doesn't exist on object class `{3}`".format( field_name, new_class, fld.attribute, new_class._meta.object_class ) )
            if field_name in new_class.declared_fields:
                continue
            if len( include_fields ) and not field_name in include_fields:
                del( new_class.base_fields[field_name] )
            if len( excludes ) and field_name in excludes:
                del( new_class.base_fields[field_name] )

        # Add in the new fields.
        new_class.base_fields.update( new_class.get_fields( include_fields, excludes ))

        return new_class


class DocumentResource( Resource ):
    '''
    A MongoEngine specific implementation of Resource
    '''
    __metaclass__ = DocumentDeclarativeMetaclass

    def _prefetch_documents( self, bundles, request ):
        # On bundles, find DBRefs in fields to prefetch
        to_fetch = {}
        for field_name, field in self.fields.items():
            # Per field, loop over the bundles and pick up related docs we could have to dereference.
            if getattr( field, 'is_related', False ) and field.to and field.full:
                related_resource = self._meta.api.resource_for_class( field.to_class )
                resource_name = related_resource._meta.resource_name

                if resource_name not in to_fetch:
                    to_fetch[ resource_name ] = set()

                if getattr( field, 'is_tomany', False ):
                    for bundle in bundles:
                        to_fetch[ resource_name ].update( bundle.obj._data[ field.attribute ] )
                else:
                    for bundle in bundles:
                        to_fetch[ resource_name ].add( bundle.obj._data[ field.attribute ] )

        for resource_name, related in to_fetch.items():
            # Limit each set to ObjectIds we can't find in the cache yet
            ids = [ ref.id for ref in related if isinstance( ref, DBRef ) and ref.id not in request.cache ]

            # Fetch the remaining ids
            if len( ids ):
                related_resource = self._meta.api.resource_by_name( resource_name )
                docs = request.cache.add( related_resource._meta.object_class.objects( id__in=ids ) )

    def _mark_relational_changes( self, bundle, obj=None ):
        '''
        @param bundle
        @type bundle: Bundle
        '''
        # Track and store any changes to relations of `obj` on the bundle.
        if obj is None:
            obj = bundle.obj

        if not RelationManagerMixin or not isinstance( obj, RelationManagerMixin ):
            return bundle

        # Find out what the RelationManagerMixin considers changed.
        to_save, to_delete = obj.get_related_documents_to_update()

        # Don't retouch stuff that will get or got updated.
        updated_by_resource = bundle.request.api['created'] | bundle.request.api['updated']
        updated_by_relationalmixin = bundle.request.api['to_save'] | bundle.request.api['to_delete'] | bundle.request.api['saved'] | bundle.request.api['deleted']
        to_save = set_difference( to_save, updated_by_resource | updated_by_relationalmixin )
        to_delete = set_difference( to_delete, updated_by_resource | updated_by_relationalmixin )

        if to_save:
            bundle.request.api['to_save'] |= to_save

        if to_delete:
            bundle.request.api['to_delete'] |= to_delete

        return bundle

    def _stash_invalid_relations( self, bundle ):
        '''
        Validates the object in the bundle. If validation fails, stashes
        invalid relational fields that are not required.

        @param bundle
        @type bundle: Bundle
        '''
        if not RelationManagerMixin or not isinstance( bundle.obj, RelationManagerMixin ):
            return bundle

        bundle.stashed_relations = {}

        try:
            bundle.obj.validate()
        except MongoEngineValidationError as e:
            for k in e.errors.keys():  # ! Document, not Resource, fields
                fld = bundle.obj._fields[k]
                if isinstance( fld, mongofields.ReferenceField ):
                    if not fld.required:
                        bundle.stashed_relations[ k ] = getattr( bundle.obj, k ).copy()
                        setattr( bundle.obj, k, None )
                elif isinstance( fld, mongofields.ListField ) and hasattr(fld, 'field'):
                    if not fld.required:
                        bundle.stashed_relations[ k ] = getattr( bundle.obj, k )[:]
                        setattr( bundle.obj, k, [] )
                else:
                    raise

        # NOTE: non-relational fields will be processed by `validate` later on
        return bundle

    def _pop_stashed_relations( self, bundle ):
        '''
        Pops any previously stashed invalid relations back on the object.

        @param bundle
        @type bundle: Bundle
        '''
        if not RelationManagerMixin or not isinstance( bundle.obj, RelationManagerMixin ):
            return bundle

        for field_name, data in bundle.stashed_relations.items():
            setattr( bundle.obj, field_name, data )

        bundle.stashed_relations = {}

        return bundle

    def _update_relations( self, bundle ):
        ''' 
        Updates any relational changes stored in the bundle.

        @type bundle: Bundle
        '''
        while bundle.request.api['to_save']:
            obj = bundle.request.api['to_save'].pop()

            if RelationManagerMixin and isinstance( bundle.obj, RelationManagerMixin ):
                # The object to be saved may induce further away updates.
                self._mark_relational_changes( bundle, obj )
                obj.save( request=bundle.request )
            else:
                obj.save()

            bundle.request.api['saved'].add(obj) 

        while bundle.request.api['to_delete']:
            obj = bundle.request.api['to_delete'].pop()

            if RelationManagerMixin and isinstance( bundle.obj, RelationManagerMixin ):
                obj.delete( request=bundle.request )
                bundle.request.api[ 'deleted' ].add( bundle.obj )
                self._mark_relational_changes( bundle, obj )
            else:
                obj.delete()
                bundle.request.api[ 'deleted' ].add( bundle.obj )

        if bundle.request.api['to_save']: 
            # Deletion may have triggered documents that need to be updated.
            # Recurse to fix relations further away incurred by the delete.
            bundle = self._update_relations( bundle )

        return bundle

    def _related_fields_callback( self, bundle, callback_func ):
        for field_name, fld in self.fields.items():

            if fld.readonly:
                continue

            if getattr( fld, 'is_related', False ) and field_name in bundle.data:
                related_data = bundle.data[ field_name ]
                if not related_data:
                    # This can happen if the field is not required and no data
                    # was given, so related_data can be None or []
                    continue
                
                related_resource = fld.get_related_resource( related_data )
                
                if not getattr( fld, 'is_tomany', False ):
                    related_data = [ related_data ]

                for related_bundle in related_data:
                    # Execute the callback function on the related resource
                    callback = getattr( related_resource, callback_func )
                    related_bundle = callback( related_bundle )

                if not getattr( fld, 'is_tomany', False ):
                    bundle.data[ field_name ] = related_data[0]

        return bundle

    def dispatch( self, request_type, request, **kwargs ):
        return super( DocumentResource, self ).dispatch( request_type, request, **kwargs )


    @classmethod
    def should_skip_field( cls, field ):
        """
        Given a MongoDB field, return if it should be included in the
        contributed ApiFields.

        By default, ignore ReferenceFields, and lists of References. These are opt-in, to avoid exposing too much data.
        """
        if isinstance( field, mongofields.ListField ) and hasattr( field, 'field' ):
            field = field.field

        if isinstance( field, mongofields.ReferenceField ):
            return True

        return False

    @classmethod
    def get_api_field_for_mongoengine_field( cls, f, default=fields.StringField ):
        """
        Returns the field type that would likely be associated with each
        MongoEngine type.

        """
        result = default  # instantiated only once by specifying it as kwarg

        # Specify only those field types that differ from default StringField
        if isinstance( f, mongofields.ObjectIdField, ):
            result = fields.ObjectIdField
        elif isinstance( f, ( mongofields.ReferenceField, mongofields.GenericReferenceField ) ):
            result = fields.ToOneField
        elif isinstance( f, mongofields.BooleanField ):
            result = fields.BooleanField
        elif isinstance( f, mongofields.FloatField ):
            result = fields.FloatField
        elif isinstance( f, mongofields.DecimalField ):
            result = fields.DecimalField
        elif isinstance( f, ( mongofields.IntField, mongofields.SequenceField ) ):
            result = fields.IntegerField
        elif isinstance( f, ( mongofields.DictField, mongofields.MapField ) ):
            result = fields.DictField
        elif isinstance( f, mongofields.EmbeddedDocumentField ):
            result = fields.EmbeddedDocumentField
        elif isinstance( f, ( mongofields.DateTimeField, mongofields.ComplexDateTimeField ) ):
            result = fields.DateTimeField
        elif isinstance( f, mongofields.ListField ) and isinstance( f.field, ( mongofields.ReferenceField, mongofields.GenericReferenceField ) ):
            result = fields.ToManyField
        elif isinstance( f, ( mongofields.ListField, mongofields.SortedListField, mongofields.GeoPointField ) ):
            # This will be lists of simple objects, since references have been
            # discarded already by should_skip_fields. 
            result = fields.ListField

        return result

    @classmethod
    def get_fields( cls, fields=None, excludes=None ):
        """
        Given any explicit fields to include and fields to exclude, add
        additional fields derived from the associated Document.
        """
        final_fields = {}
        fields = fields or []
        excludes = excludes or []

        if not cls._meta.object_class:
            return final_fields

        for name, f in cls._meta.object_class._fields.items():
            # If the field name is already present, skip
            if name in cls.base_fields:
                continue

            # If field is not present in explicit field listing, skip
            if fields and name not in fields:
                continue

            # If field is in exclude list, skip
            if excludes and name in excludes:
                continue

            # Skip fields for which `should_skip_field` returns True. By default, ReferenceFields are omitted.
            if cls.should_skip_field( f ):
                continue

            api_field_class = cls.get_api_field_for_mongoengine_field( f )

            kwargs = {
                'attribute': f.name,
                'help_text': f.help_text,
            }

            if f.required is True:
                kwargs['required'] = True

            kwargs['unique'] = f.unique

            if f.default is not None:
                kwargs['default'] = f.default

            if getattr( f, 'auto_now', False ):
                kwargs['default'] = f.auto_now

            if getattr( f, 'auto_now_add', False ):
                kwargs['default'] = f.auto_now_add

            final_fields[name] = api_field_class( **kwargs )
            final_fields[name].contribute_to_class( cls, name )

        return final_fields


    def dehydrate_id( self, bundle ):
        '''
        pk is present on objects, but not a MongoEngine field. Hence we need to
        explicitly dehydrate it since it won't be included in _fields.
        '''
        return bundle.obj.pk

    def get_resource_uri( self, request, data=None, absolute=False ):
        """
        Returns the resource's relative or absolute uri per the given API.
        """
        kwargs = {
            'resource_name': self._meta.resource_name,
            'absolute': not not absolute if absolute else self._meta.use_absolute_uris,
        }

        if not data:
            kwargs[ 'operation' ] = 'list'
        else:
            kwargs['operation'] = 'single'

            # Try to rip an id out of the data
            if isinstance( data, Bundle ):
                if data.obj and isinstance( data.obj, Document ):
                    data = data.obj
                else:
                    data = data.data

            if isinstance( data, dict ):
                if '_ref' in data:
                    # GenericReference straight from _data
                    data = data['_ref']

            if isinstance( data, Document ):
                kwargs['id'] = getattr( data, 'pk', None )

            elif isinstance( data, DBRef ):
                kwargs[ 'id' ] = data.id  # returns an ObjectId

            elif isinstance( data, ObjectId ):
                kwargs[ 'id' ] = str( data )

            elif isinstance( data, basestring ):
                # assume the data _is_ the URI
                kwargs[ 'id' ] = self._meta.api.get_id_from_resource_uri( data )

        return self._meta.api.build_uri( request, **kwargs )

    def apply_ordering( self, obj_list, options=None ):
        """
        Given a dictionary of options, apply some ODM-level ordering to the
        provided `QuerySet`.

        Looks for the `order_by` key and handles either ascending ( just the
        field name ) or descending ( the field name with a `-` in front ).
        """
        if options is None:
            options = {}

        if not 'order_by' in options:
            # Nothing to alter the ordering. Return what we've got.
            return obj_list

        order_by_args = []

        order_bits = options['order_by']
        if not isinstance( order_bits, ( list, tuple )):
            order_bits = [order_bits]

        for order_bit in order_bits:
            order_by_bits = order_bit.split( LOOKUP_SEP )

            field_name = order_by_bits[0]
            order = ''

            if order_by_bits[0].startswith( '-' ):
                field_name = order_by_bits[0][1:]
                order = '-'

            if not field_name in self.fields:
                # It's not a field we know about. Move along citizen.
                raise InvalidSortError( "No matching `{0}` field for ordering on.".format(field_name) )

            if not field_name in self._meta.ordering:
                raise InvalidSortError( "The `{0}` field does not allow ordering.".format(field_name) )

            if self.fields[field_name].attribute is None:
                raise InvalidSortError( "The `{0}` field has no 'attribute' for ordering with.".format(field_name) )

            order_by_args.append( "{0}{1}".format( order, LOOKUP_SEP.join( [self.fields[field_name].attribute] + order_by_bits[1:] ) ) )

        return obj_list.order_by( *order_by_args )

    def get_api_field_for_document_field( self, field_name ):
        """
        Given a field name, we can find this field name on the document, and return an api field.
        """
        if field_name in self.Meta.object_class._fields:
            related_field = self.get_api_field_for_mongoengine_field( self.Meta.object_class._fields[ field_name ] )
            related_field = related_field( attribute=field_name )
            related_field.contribute_to_class( self.__class__, field_name )
            return related_field
        else:
            return None


    def check_filtering( self, field, filter_type='exact', filter_bits=None ):
        """
        Given a field name, an optional filter type and an optional list of
        additional relations, determine if a field can be filtered on.

        If a filter does not meet the needed conditions, it should raise an
        `InvalidFilterError`.

        If the filter meets the conditions, a list of tuples of the form
        [ ( Resource, Field ), ... ]  is returned.
        """
        if filter_bits is None:
            filter_bits = []

        # Check to see if it's a relational lookup and if that's allowed.
        if len( filter_bits ):
            if not getattr( field, 'is_related', False ):
                raise InvalidFilterError( "The '%s' field does not support relations." % field.field_name )

            if self._meta.filtering.get( field.field_name ) != ALL_WITH_RELATIONS:
                raise InvalidFilterError( "Lookups are not allowed more than one level deep on the '%s' field." % field.field_name )

            related_resource = field.get_related_resource( None )
            if filter_bits[0] in related_resource.fields:
                related_field = related_resource.fields[ filter_bits[0] ]
            else:
                # then this field does not exist on the related resource
                if filter_bits[0] in related_resource._meta.filtering:
                    # then this field is allowed to be filtered on, even though its excluded from the resource, this
                    # means that we probably want to filter on a field of the document, which we create here:
                    related_field = related_resource.get_api_field_for_document_field( filter_bits[0] )

                if not filter_bits[0] in related_resource._meta.filtering or not related_field:
                    # then this field does not exist on either the resource or the document, or it does not allow
                    # filtering either
                    raise InvalidFilterError( "`{0}` is not a field on `{1}`".format( filter_bits[0], field.field_name ) )

            # Recursively descend through the remaining lookups in the filter, if any. By recursively calling
            # check_filtering, we ensure that on every level, the lookup is allowed.
            return [ ( self, field ) ] + related_resource.check_filtering( related_field, filter_type, filter_bits[1:] )

        invalid_filter = False
        # filter out forbidden filtering methods for list dict and embeddedDocument fields:
        if isinstance( field, ( fields.ListField, fields.DictField, fields.EmbeddedDocumentField ) ) and filter_type not in ( 'exists', 'size' ):
            invalid_filter = True
        # filter out forbidden filtering methods for the size operator:
        if filter_type == 'size' and not isinstance( field, ( fields.ListField, fields.DictField, fields.EmbeddedDocumentField, fields.ToManyField ) ):
            invalid_filter = True
        # take care that the string operators only are used for string fields:
        if filter_type in QUERY_MATCH_OPERATORS and not isinstance( field, fields.StringField ):
            invalid_filter = True

        # look at the resource specific limitations on filtering, defined in this resource's meta:
        if not field.field_name in self._meta.filtering:
            raise InvalidFilterError( "The `{0}` field does not allow filtering.".format( field.field_name ) )
        elif self._meta.filtering[ field.field_name ] not in ( ALL, ALL_WITH_RELATIONS ):
            if filter_type not in self._meta.filtering[ field.field_name ]:
                invalid_filter = True

        if invalid_filter:
            raise InvalidFilterError( "The `{0}` field does not allow filtering with '{1}'.".format( field.field_name, filter_type ) )

        if field.attribute is None:
            raise InvalidFilterError( "The `{0}` field has no 'attribute' to apply a filter on.".format( field.field_name ) )

        return [ ( self, field ) ]

    def parse_filter_value( self, value, field, filter_type ):
        """
        Turn the string or list of strings `value` into a python object.
        @type value: string or list<string>
        """
        try:
            if filter_type == 'size':
                # takes an int
                value = int( value )
            elif filter_type == 'exists':
                # takes a boolean
                if value in ( 'true', 'True', 't', '1', True ):
                    value = True
                elif value in ( 'false', 'False', 'f', '0', False ):
                    value = False
                else:
                    value = None
            elif filter_type in QUERY_MATCH_OPERATORS:
                # these query operators work only on strings
                value = unicode( value )
            elif filter_type in QUERY_EQUALITY_OPERATORS and not isinstance( field, fields.ToManyField ):
                if not isinstance( field, fields.StringField ) and value in ( '', 'nil', 'null', 'none', 'None', None ):
                    value = None
                else:
                    # then the value should be of the same type as field, so we can use the field's convert function:
                    value = field.convert_from_string( value )
            elif filter_type in QUERY_LIST_OPERATORS or isinstance( field, fields.ToManyField ):
                # then the value should be a list of elements of the same type as field
                if not isinstance( value, list ):
                    # with a single value it is possible that webob did not create a list
                    value = [ value ]

                if not isinstance( field, fields.StringField ):
                    value = [ field.convert_from_string( elem ) for elem in value if elem not in ( '', 'nil', 'null', 'none', 'None', None ) ]
                else:
                    value = [ field.convert_from_string( elem ) for elem in value ]
        except:
            raise InvalidFilterError( "The filter type `{}` on the `{}` field does not accept the value `{}`".format(
                filter_type, field.field_name, value) )

        return value

    def build_filters( self, filters, request ):
        """
        Given a dictionary of filters, creates the corresponding ODM filters,
        checking whether filtering on the field is allowed in the Resource
        definition.

        Valid values are either a list of MongoEngine filter types ( i.e.
        `['startswith', 'exact', 'lte', 'icontains']` ) the `ALL` or 
        'ALL_WITH_RELATIONS' constants.

        The resource needs to explicitly allow filtering on its fields either by specifying the resource's filtering as
        a list:

        filtering = [ 'resource_field_name_1', 'resource_field_name_2', 'resource_field_name_4' ],

        meaning these fields allow all possible types of filtering, or as a dict, indicating which specific filters are
        allowed on each field:

        filtering = {
            'resource_field_name_1': ['exact', 'startswith', 'endswith', 'contains'],
            'resource_field_name_2': ['exact', 'gt', 'gte', 'lt', 'lte', 'range'],
            'resource_field_name_3': ALL,
            'resource_field_name_4': ALL_WITH_RELATIONS,
            ...
        }

        ( Note that if filtering is specified as list, it is rebuilt to a dict when instantiating the resource )

        Creates cross-relational lookups by using the results of intermediary
        filters on related resources.

        You can prepend field names with `OR__` to create an or filter. 
        Functionality is limited since all OR's are combined and OR'ed with all
        non-OR conditions, there's no support for nested ORs and ANDs.

        Returns a Q (combination) object that can be used in <queryset>.filter()
        """
        if not filters:
            filters = {}

        and_filters = []
        or_filters = []

        for filter_expr, value in filters.items():
            filter_bits = filter_expr.split( LOOKUP_SEP )
            filter_type = 'exact'  # default
            field_name = filter_bits.pop( 0 )

            is_or_filter = ( field_name == 'OR' )
            if is_or_filter:
                field_name = filter_bits.pop( 0 )

            if field_name not in self.fields:
                if field_name in self._meta.filtering:
                    # then this field is allowed to be filtered on, even though its excluded from the resource, this
                    # means that we probably want to filter on a field of the document, which we create here:
                    field = self.get_api_field_for_document_field( field_name )
                else:
                    # Not a field the Resource knows about, so ignore it.
                    continue
            else:
                field = self.fields[ field_name ]

            # Override filter_type if it is given.
            if len( filter_bits ) and filter_bits[-1].replace('[]', '') in QUERY_TERMS:
                filter_type = filter_bits.pop().replace('[]', '')

            # Example:
            # Books.filter( author__name__icontains='Fred' ) receives:
            # [ (BookResource, 'author'), (AuthorResource, 'name') ], from 
            # `check_filtering`, for which we return the filter:
            #   { 'author__id__in': author_ids }
            # where `author_ids` is the result set from
            #   AuthorResource.filter( name__icontains='Fred' )
            resource_filters = self.check_filtering( field, filter_type, filter_bits )
            value = resource_filters[-1][0].parse_filter_value( value, resource_filters[-1][1], filter_type )

            if len( resource_filters ) > 1:
                # Traverse related fields backwards, creating lists of ids as
                # intermediate results to limit nearer Documents to.
                for ( resource, field ) in reversed( resource_filters[1:] ):
                    resource_filter = { "{0}{1}{2}".format( field.field_name, LOOKUP_SEP, filter_type ): value }
                    # Use the results for this resource for the next query.
                    filter_type = 'in'
                    value = [ str(d['_id']) for d in resource.obj_get_list( request, **resource_filter ).only( 'id' ).as_pymongo() ]

            # Return the queryset filter
            qs_filter = "{0}{1}{2}".format( resource_filters[0][1].attribute, LOOKUP_SEP, filter_type )

            if is_or_filter:
                or_filters.append( ( qs_filter, value ) )
            else:
                and_filters.append( ( qs_filter, value ) )

        # Combine `filter_group` into a (set of) Q filters. A single Q object is `and`; for `or`, we create a set of filters.
        q_filter = Q()

        if and_filters:
            q_filter = reduce( and_, ( Q(**{k:v}) for k,v in and_filters ) )

        if or_filters:
            q_filter &= reduce( or_, ( Q(**{k:v}) for k,v in or_filters ) )

        return q_filter

    def get_queryset( self, request ):
        qs = None

        if hasattr( self._meta, 'queryset' ) and self._meta.queryset:
            qs = self._meta.queryset.clone()
        elif hasattr( self._meta, 'object_class' ):
            qs = self._meta.object_class.objects()

        if qs is None:
            raise NotImplementedError('Resource needs a `queryset` or `object_class` to return objects')

        return qs

    def save( self, bundle ):
        """
        Creates or updates a Resource including any nested Resources in the bundle.

        Returns a bundle with the new or updated objects, their data ready to 
        be deserialized and any errors that occured along the way.

        First create any new resources in the bundle. This is done to ensure 
        that all related resources exist. Then validate the resource tree, 
        and finally save all updated documents.
        """
        self._prepare_request( bundle.request )

        bundle = self.save_new( bundle )
        bundle = self.validate( bundle )
        bundle = self.update( bundle )
        bundle = self._update_relations( bundle )

        return bundle

    def save_new( self, bundle ):
        '''
        Creates objects in the bundle tree that don't have a primary key yet.

        Introspects invalidating fields to see if they can be made to validate
        by staging the creation of related objects.
        
        Relations are always defined as two-way links between resources.

        It can resolve in case:
         - there is a single relation between the parent and child resources 
           for which none or only one link is required
         - there are multiple relations between the resources, but only 1 link 
           is required in one direction: assume this is the intended relation

        It obviously *doesn't* work if:
         - there is a relation required by both ends (deadlock). 
           This should however already be prevented during class instantiation.
         - there are multiple relations between the parent and child resources 
           but none is required (how should we choose?)

        NOTE: 
        The foregoing hydrate phase has already established all links between
        existing objects, so here we're only concerned with objects that
        don't have a primary key yet and must be assumed new.
        
        .. Example: 2 new objects that are related: `organization` and `owner`.

            { name: 'ACME', 
              owner: { name: 'boss' } }

            Validating `organization` fails because `owner` doesn't exist yet.
            We have one of 2 cases (can't be both):
            I) `owner` requires an organization
            II) `organization` requires an owner

            Heuristics 
            ===

            phase 1) `ACME`
            ---

            - Validate `organization` and stash any invalid, not-required,
              related fields (in case I: `owner`) 
            - try to save `organization` 
            - for case I, `ACME` will have been created now.
            - for case II, we still have to wait a bit till phase 3.
            - pop the relations back on because we need them down the line.

              Bail out if there are validationerrors on non-related fields: 
              we won't be able to resolve invalid data being passed in.

            phase 2) `ACME`
            ---

            - recurse down to nested objects passing our annotated self along.

            phase 1) `boss`
            ---

            - for case I, no problem, save ourself.
            - for case II, `owner` won't validate: it has a required link to
              `organization`, but `ACME` doesn't have a pk yet.

            phase 2) `boss`
            ---

            - no further nested bundles to traverse for either case.

            phase 3) `boss`
            ---

            - for case I, we have a pk here so just return.
            - for case II, we don't have a pk yet. Validate ourself again and
              verify that there's only 1 invalid field left (`organization`)
              and that that field corresponds to our `parent`. Stash it
              temporarily, save ourself and pop it back on if succesful.
            - If validation still fails we're out of luck (shouldn't happen
              here since we would've bailed out earlier).

            phase 3) `ACME`
            ---

            - we're done for case I.
            - for case II, we don't have a pk yet. But validation and saving
              should be fine now that all others have received an id. 
            - If validation still fails or if we encountered errors along the
              way we're out of luck.

        '''
        # PHASE 1: If we're brand spankin' new try to get us an id.
        if not bundle.obj.pk:
            bundle = self._mark_relational_changes( bundle )
            bundle = self._stash_invalid_relations( bundle )

            try:
                if RelationManagerMixin and isinstance( bundle.obj, RelationManagerMixin ):
                    bundle.obj.save( request=bundle.request )
                else:
                    bundle.obj.save()

                bundle.request.api['created'].add( bundle.obj )

            except MongoEngineValidationError, e:
                # We'll have to wait for related objects to be created first.
                pass
            bundle = self._pop_stashed_relations( bundle )

        # PHASE 2: Recurse for any nested related resources.
        bundle = self._related_fields_callback( bundle, 'save_new' )

        # PHASE 3: Second attempt to create the object now its relations exist.
        if not bundle.obj.pk:
            if RelationManagerMixin and isinstance( bundle.obj, RelationManagerMixin ):
                bundle = self._mark_relational_changes( bundle )
                bundle.obj.save( request=bundle.request )
            else:
                bundle.obj.save()

            bundle.request.api['created'].add( bundle.obj )

        return bundle

    def validate( self, bundle ):
        '''
        Recursively validates the (embedded) object(s) in the bundle.
        Does not change any data nor update the database. Returns the bundle.

        Adds validationerrors to the bundle so previously created objects may
        be rolled back.
        '''

        # PHASE 4: All objects now exist and all relations are assigned, so
        # everything should validate. 
        bundle.obj.validate()

        return self._related_fields_callback( bundle, 'validate' )

    def update( self, bundle ):
        '''
        Recursively updates the (embedded) object(s) in the validated bundle.
        '''
        if bundle.uri_only:
            # Don't update here. May get updated through _update_relations.
            return bundle

        if RelationManagerMixin and isinstance( bundle.obj, RelationManagerMixin ):
            bundle = self._mark_relational_changes( bundle )
            bundle.obj.save( request=bundle.request, validate=False )
        else:
            bundle.obj.save( validate=False )

        bundle.request.api['updated'].add( bundle.obj )

        return self._related_fields_callback( bundle, 'update' )

    def obj_get_list( self, request, **kwargs ):
        """
        A Pyramid/MongoEngine implementation of `obj_get_list`.
        """
        if kwargs:
            filters = kwargs.copy()
        elif request and hasattr( request, 'GET' ):
            # Pyramid's Request object uses a Multidict for its representation.
            # Transform this into an 'ordinary' dict for further processing.
            filters = request.GET.mixed()
        else:
            filters = None

        q_filter = self.build_filters( filters, request )

        try:
            return self.get_queryset( request ).filter( q_filter )
        except ValueError:
            raise BadRequest( "Invalid resource lookup data provided (mismatched type)." )

    def obj_get_single( self, request, **kwargs ):
        """
        A Pyramid/MongoEngine specific implementation of `obj_get_single`.

        Uses `obj_get_list` to get the initial list, which should contain
        only one instance matched by the request and the provided `kwargs`.
        """
        filters = {}
        obj = None

        id = kwargs.get( 'pk' ) or kwargs.get( 'id' )
        if not id and 'uri' in kwargs:
             # We have received a uri. Try to grab an id from it.
            id = self._meta.api.get_id_from_resource_uri( kwargs.get( 'uri', '' ) )

        if id:
            # Try to fetch the object from the document cache
            if hasattr( request, 'cache' ):
                obj = request.cache.get( id, None )

            if not obj:
                # Object not found in cache, so add a filter for its id
                filters['id'] = id
        else:
            filters = kwargs.copy()

        # Object not in cache, alas, we have to hit the database.
        if not obj:
            matched = [ o for o in self.obj_get_list( request, **filters ) ]
            if len( matched ) == 1:
                obj = matched[ 0 ]

            if not obj:
                # Filters returned 0 or more than 1 match, raise an error.
                stringified_filters = ', '.join( ["{0}={1}".format( k, v ) for k, v in filters.items()] )
                if len(matched) == 0:
                    raise self._meta.object_class.DoesNotExist( "Couldn't find an instance of `{0}` which matched `{1}`.".format( self._meta.object_class.__name__, stringified_filters ) )
                else:
                    raise self._meta.object_class.MultipleObjectsReturned( "More than one `{0}` matched `{1}`.".format( self._meta.object_class.__name__, stringified_filters ) )

        return obj

    def obj_delete_list( self, request, **kwargs ):
        """
        Tries to retrieve a set of resources per the given `request` and 
        `kwargs` and deletes them if all are found. 

        Returns `HTTPNoContent` if successful, or `HTTPNotFound`.
        """
        objects = self.obj_get_list(request, **kwargs)
        bundles = [ self.build_bundle( request=request, obj=obj ) for obj in objects ]
        for bundle in bundles:
            if bundle:
                request.api['to_delete'].add( bundle.obj )
                self._update_relations( bundle )

    def obj_delete_single( self, request, **kwargs ):
        """
        Tries to retrieve a resource per the given `request` and `kwargs` and
        deletes it if found. 

        Returns `HTTPNoContent` if successful, or `HTTPNotFound`.
        """
        try:
            obj = self.obj_get_single(request, **kwargs)
        except DoesNotExist:
            raise NotFound("A model instance matching the provided arguments could not be found.")

        bundle = self.build_bundle( request=request, obj=obj )
        if bundle:
            request.api['to_delete'].add( bundle.obj )
            self._update_relations( bundle )