from __future__ import print_function
from __future__ import unicode_literals

from . import fields
from . import http
from .serializers import Serializer
from .exceptions import *
from .constants import ALL, ALL_WITH_RELATIONS, QUERY_TERMS, LOOKUP_SEP
from .utils import determine_format, build_content_type
from .bundle import Bundle
from .authentication import Authentication
from .throttle import BaseThrottle
from .paginator import Paginator

from pyramid.response import Response
from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned
from mongoengine.base import ValidationError as MongoEngineValidationError
import mongoengine.document 
import mongoengine.fields as mf

from copy import deepcopy

class ResourceOptions( object ):
    """
    A configuration class for `Resource`.

    Provides sane defaults and the logic needed to augment these settings with
    the internal `class Meta` used on `Resource` subclasses.
    """
    serializer = Serializer()
    authentication = Authentication()
    throttle = BaseThrottle()
    allowed_methods = [ 'get', 'post', 'put', 'delete' ]
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

        allowed_methods = overrides.get( 'allowed_methods', [ 'get', 'post', 'put', 'delete' ] )

        if overrides.get( 'list_allowed_methods', None ) is None:
            overrides['list_allowed_methods'] = allowed_methods

        if overrides.get( 'single_allowed_methods', None ) is None:
            overrides['single_allowed_methods'] = allowed_methods

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
                    attrs['base_fields'][field_name] = deepcopy( fld )
        except NameError:
            pass

        # Find fields explicitly set on the Resource
        for field_name, obj in attrs.items():
            if isinstance( obj, fields.ApiField ):
                field = attrs.pop( field_name )
                declared_fields[field_name] = field

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
        self.fields = deepcopy( self.base_fields )

        if not api is None:
            self._meta.api = api

    def __getattr__( self, name ):
        if name in self.fields:
            return self.fields[name]
        raise AttributeError( name )


    def get_resource_uri( self, request, bundle_or_object = None, absolute=None ):
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
        if self._meta.throttle.should_be_throttled( identifier ):
            # Throttle limit exceeded.
            raise ImmediateHTTPResponse( response=http.HTTPForbidden() )

    def log_throttled_access(self, request):
        """
        Handles the recording of the user's access for throttling purposes.

        Mostly a hook, this uses class assigned to `throttle` from
        `Resource._meta`.
        """
        request_method = request.method.lower()
        self._meta.throttle.accessed( self._meta.authentication.get_identifier(request), url=request.path_url, request_method=request_method )

    def create_response( self, data, request=None, response_class=Response, **response_kwargs ):
        """
        Extracts the common "which-format/serialize/return-response" cycle.
        """
        if request:
            desired_format = self.determine_format( request )
        else:
            desired_format = self._meta.default_format

        serialized = self.serialize( request, data, desired_format )
        return response_class( body=serialized, content_type=build_content_type( desired_format ), **response_kwargs )



    def deserialize( self, request, data, format=None ):
        """
        Analogous to python 'unpickle': translates serialized `data` in a given 
        `format` to python data structures.

        It relies on the request properly sending a `CONTENT_TYPE` header,
        falling back to the default format if not provided.

        Mostly a hook, this uses the `Serializer` from `Resource._meta`.
        """
        format = format or request.content_type or self._meta.default_format
        return self._meta.serializer.deserialize( data, format )

    def post_deserialize_list( self, data, request ):
        """
        A hook to alter data just after it has been received from the user &
        gets deserialized.

        Useful for altering the user data before any hydration is applied.
        """
        return data

    def post_deserialize_single( self, data, request ):
        """
        A hook to alter data just after it has been received from the user &
        gets deserialized.

        Useful for altering the user data before any hydration is applied.
        """
        return data

    def build_bundle( self, obj=None, data=None, request=None ):
        """
        Given either an object, a data dictionary or both, builds a `Bundle`
        for use throughout the `dehydrate/hydrate` cycle.

        If no object is provided, an empty object from
        `Resource._meta.object_class` is created so that attempts to access
        `bundle.obj` do not fail (i.e. during validation/hydration)
        """
        if obj is None:
            obj = self._meta.object_class()

        bundle = Bundle( obj=obj, data=data, request=request )
        return bundle

    def bundle_from_uri( self, uri, request=None ):
        """
        Given a URI is provided, the resource is attempted to be loaded and put
        in a fresh bundle.
        """
        bundle = self.build_bundle( data={'resource_uri': uri}, request=request )
        bundle.obj = self.obj_get_single( request=request, uri=uri )

        bundle.from_uri = True
        return bundle

    def bundle_from_data( self, data, request=None ):
        """
        Given a dictionary-like structure is provided, a fresh bundle is 
        created using that data.

        If the data contains a resource_uri, any other keys in the data are 
        assumed to be updates to the existing object's properties.
        If the data contains no resource_uri, a new object is instantiated.

        Errors are added to the bundle if a new resource may not be created or 
        if an existing resource is not found or may not be updated.
        """
        #assert isinstance( data, dict )

        if 'resource_uri' in data:
            # We seem to be wanting to modify an existing resource. 
            # Try to retrieve the object and put it in fresh bundle.
            bundle = self.bundle_from_uri( uri=data['resource_uri'], request=request )
            bundle.data = data
        else:
            # No resource_uri in data. Create a fresh bundle for it.
            bundle = self.build_bundle( data=data, request=request )

        bundle.from_data = True
        return bundle

    def pre_hydrate( self, bundle ):
        '''
        A hook for allowing some custom hydration on the data specific to this
        resource before each field's hydrate function is called.
        '''
        return bundle

    def hydrate( self, bundle ):
        """
        Takes data from the resource and converts it to a form ready to be 
        stored on objects. Returns a fully hydrated bundle.

        Creates related bundles for related fields, instantiating corresponding
        objects along the way and nesting them in the data for the main bundle. 

        The result of the hydrate function is a fully populated bundle with 
        nested bundles for related objects. 

        Also stores ids of formerly related objects in the bundle. These need
        to be saved later because their privileges or embedded references will 
        have changed.

        Any errors encountered in the data for the related objects are
        propagated to the parent's bundle.
        """

        if bundle.obj is None:
            bundle.obj = self._meta.object_class()

        bundle = self.pre_hydrate( bundle )

        for field_name, fld in self.fields.items():
            method = getattr(self, "hydrate_{0}".format(field_name), None)

            if method:
                # A custom `hydrate_foo` method may provide data for the field
                data = method( bundle )
            else:
                # Hydrate the data for the field. Recurses for related resources.
                data = fld.get_data( bundle )

            # Replace the data for the field with its hydrated version.
            bundle.data[ field_name ] = data

            if data is None:
                continue

            if getattr(fld, 'is_related', False): 
                if getattr(fld, 'is_tomany', False):
                    #assert isinstance( data, list )
                    # Save our current relations before setting new ones.

                    # Now set the new ones.
                    setattr( bundle.obj, fld.attribute, [b.obj for b in data] )
                    related_errors = [b.errors for b in data if b.errors]
                    if related_errors:
                        bundle.errors[ field_name ] = related_errors
                else:
                    #assert isinstance( data, Bundle )
                    setattr( bundle.obj, fld.attribute, data.obj )
                    if data.errors:
                        bundle.errors[ field_name ] = data.errors 

            else:
                if fld.attribute:
                    setattr( bundle.obj, fld.attribute, data )

        return bundle

    def save_new( self, bundle ):
        raise NotImplementedError()

    def validate( self, bundle ):
        raise NotImplementedError()

    def update( self, bundle ):
        raise NotImplementedError()

    def dehydrate( self, bundle ):
        """
        Given a bundle with an object instance, extract the information from 
        it to populate the resource data.
        """
        # Dehydrate each field.
        for field_name, fld in self.fields.items():
            bundle.data[field_name] = fld.create_data( bundle )

            # Check for an optional method to do further dehydration.
            method = getattr( self, "dehydrate_{0}".format(field_name), None )
            if method:
                bundle.data[field_name] = method( bundle )

        return self.post_dehydrate( bundle )

    def post_dehydrate( self, bundle ):
        '''
        A hook for allowing some custom dehydration on the whole resource after 
        each field's dehydrate function has been called.
        '''
        return bundle

    def pre_serialize_list( self, bundles_list, request ):
        """
        A hook to alter data just before it gets serialized & sent to the user.

        Useful for restructuring/renaming aspects of the what's going to be
        sent.
        """
        return bundles_list

    def pre_serialize_single( self, bundle, request ):
        """
        A hook to alter data just before it gets serialized & sent to the user.

        Useful for restructuring/renaming aspects of the what's going to be
        sent.
        """
        return bundle

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
        allowed_methods = getattr( self._meta, '{0}_allowed_methods'.format(request_type), None )
        request_method = self.check_method( request, allowed=allowed_methods )
        print( 'resource={0}; request={1}_{2}'.format( self._meta.resource_name, request_method, request_type ) )

        # Determine which callback we're going to use
        method = getattr( self, '{0}_{1}'.format( request_method, request_type ), None )
        if method is None:
            error = 'Method="{0}_{1}" is not implemented for resource="{2}"'.format( request_method, request_type, self._meta.resource_name )
            raise ImmediateHTTPResponse( response=http.HTTPNotImplemented( body=error ))

        self.is_authenticated( request )
        self.check_throttle( request )

        # All clear. Process the request.
        response = method( request, **kwargs )
        self.log_throttled_access(request)

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
        return self.create_response( self.build_schema(), request )

    def get_list( self, request ):
        """
        Returns a serialized list of resources.

        Calls `obj_get_list` to provide the objects to be dehydrated
        and serialized.

        Should return an HTTPResponse ( 200 OK ).
        """
        objects = self.obj_get_list( request=request, **request.matchdict )

        paginator = self._meta.paginator_class(
                request.GET, 
                objects, 
                resource_uri=self.get_resource_uri( request ), 
                limit=self._meta.limit, 
                max_limit=self._meta.max_limit, 
                )
        data = paginator.page()

        # Create a bundle for every object and dehydrate those bundles individually
        bundles = [self.build_bundle( obj=object, request=request ) for object in data['objects']]
        bundles = [self.dehydrate( bundle ) for bundle in bundles]
        data['objects'] = self.pre_serialize_list( bundles, request )
        return self.create_response( data, request )

    def get_single( self, request ):
        """
        Returns a single serialized resource.

        Calls `obj_get_single` to provide the object to be dehydrated
        and serialized.

        Should return an HTTPResponse ( 200 OK ).
        """
        try:
            object = self.obj_get_single( request=request, **request.matchdict )
        except DoesNotExist:
            return http.HTTPNotFound()
        except MultipleObjectsReturned:
            return http.HTTPMultipleChoices( "More than one resource is found at this URI." )

        bundle = self.build_bundle( obj=object, request=request )
        bundle = self.dehydrate( bundle )
        data = self.pre_serialize_single( bundle, request )
        return self.create_response( data, request )

    def save( self, bundle ):
        """
        Creates or updates a Resource including any nested Resources in the bundle.

        Returns a bundle with the new or updated objects, their data ready to 
        be deserialized and any errors that occured along the way.
        """
        # Hydrate parses the data recursively, looking up or instantiating
        # nested objects along the way and replacing related resources data
        # with related bundles.
        bundle = self.hydrate( bundle )

        # First create any new resources in the bundle. 
        # This is done to ensure that all related resources exist.
        # Then validate the resource tree, and save updated documents.
        bundle = self.save_new( bundle )
        bundle = self.validate( bundle )
        bundle = self.update( bundle )

        return bundle

    def post_list( self, request, **kwargs ):
        """
        Creates a new Resource.

        Returns `HTTPCreated` (201 Created) if all went well.
        Returns `HTTPBadRequest` (500) with any errors that occurred.
        """
        data = self.deserialize( request, request.body, format=request.content_type )
        data = self.post_deserialize_single( data, request )

        bundle = self.bundle_from_data( data=data, request=request )
        bundle = self.save( bundle )

        location = self.get_resource_uri( request )
        if self._meta.return_data_on_post:
            # Re-populate the data from the objects.
            bundle = self.dehydrate(bundle)
            data = self.pre_serialize_single( bundle, request )
            return self.create_response( data, request, response_class=http.HTTPCreated, location=location )
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
        data = self.post_deserialize_list( data, request )

        bundles = []
        for item in data:
            bundle = self.bundle_from_data( data=item, request=request )
            bundles.append( self.save( bundle ) )

        if self._meta.return_data_on_put:
            # Re-populate the data from the objects.
            bundles = [self.dehydrate( bundle ) for bundle in bundles]
            data = {'objects': self.pre_serialize_list( bundles, request )}
            return self.create_response( data, request, response_class=http.HTTPAccepted )
        else:
            return http.HTTPNoContent()

    def put_single(self, request, **kwargs):
        """
        Updates an existing Resource.

        Returns `HTTPAccepted` (204) if all went well, or `HTTPNoContent` (204)
        if return data was also requested.
        Returns `HTTPBadRequest` (500) with any errors that occurred.
        """
        data = self.deserialize( request, request.body, format=request.content_type )
        data = self.post_deserialize_single( data, request )

        bundle = self.bundle_from_data( data=data, request=request )
        bundle = self.save( bundle )

        location = self.get_resource_uri( request, bundle )
        if self._meta.return_data_on_put:
            # Re-populate the data from the objects.
            bundle = self.dehydrate( bundle )
            data = self.pre_serialize_single( bundle, request )
            return self.create_response( data, request, response_class=http.HTTPAccepted, location=location )
        else:
            return http.HTTPNoContent( location=location )
        
    def delete_list(self, request, **kwargs):
        """
        Destroys a collection of resources/documents.

        Calls ``obj_delete_list``.

        If the resources are deleted, return ``HttpNoContent`` (204 No Content).
        """
        self.obj_delete_list(request=request, **self.remove_api_resource_names(kwargs))
        return http.HttpNoContent()

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

    def check_filtering( self, field_name, filter_type='exact', filter_bits=None ):
        """
        Given a field name, an optional filter type and an optional list of
        additional relations, determine if a field can be filtered on.

        If a filter does not meet the needed conditions, it should raise an
        `InvalidFilterError`.

        If the filter meets the conditions, a list of attribute names ( not
        field names ) will be returned.
        """
        if filter_bits is None:
            filter_bits = []

        if not field_name in self._meta.filtering:
            raise InvalidFilterError( "The `{0}` field does not allow filtering.".format(field_name) )

        # Check to see if it's an allowed lookup type.
        if not self._meta.filtering[field_name] in ( ALL, ALL_WITH_RELATIONS ):
            # Must be an explicit whitelist.
            if not filter_type in self._meta.filtering[field_name]:
                raise InvalidFilterError( "`{0}` is not an allowed filter on the `{1}` field.".format( filter_type, field_name ))

        if self.fields[field_name].attribute is None:
            raise InvalidFilterError( "The `{0}` field has no 'attribute' to apply a filter on.".format(field_name) )

        # Check to see if it's a relational lookup and if that's allowed.
        if len( filter_bits ):
            if not getattr( self.fields[field_name], 'is_related', False ):
                raise InvalidFilterError( "The `{0}` field does not support relations.".format(field_name) )

            if not self._meta.filtering[field_name] == ALL_WITH_RELATIONS:
                raise InvalidFilterError( "Lookups are not allowed more than one level deep on the `{0}` field.".format(field_name) )

            # Recursively descend through the remaining lookups in the filter,
            # if any. We should ensure that all along the way, we're allowed
            # to filter on that field by the related resource.
            related_resource = self.fields[field_name].get_related_resource()
            return [self.fields[field_name].attribute] + related_resource.check_filtering( filter_bits[0], filter_type, filter_bits[1:] )

        return [self.fields[field_name].attribute]

    def filter_value_to_python( self, value, field_name, filters, filter_expr, filter_type ):
        """
        Turn the string `value` into a python object.
        """
        # Simple values
        if value in ['true', 'True', True]:
            value = True
        elif value in ['false', 'False', False]:
            value = False
        elif value in ( 'nil', 'none', 'None', None ):
            value = None

        # Split on ',' if not empty string and either an in or range filter.
        if filter_type in ( 'in', 'range' ) and len( value ):
            if hasattr( filters, 'getlist' ):
                value = []

                for part in filters.getlist( filter_expr ):
                    value.extend( part.split( ',' ))
            else:
                value = value.split( ',' )

        return value



    def obj_get_single( self, request=None, **kwargs ):
        """
        Fetches a single object at a given resource_uri.

        This needs to be implemented at the user level. 
        This should raise `NotFound` or `MultipleObjects` exceptions
        when there's no or multiple objects at the resource_uri.
        """
        raise NotImplementedError()

    def obj_get_list( self, request=None, **kwargs ):
        """
        Fetches a list of objects at a given resource_uri.

        This needs to be implemented at the user level.
        Returns an empty list if there are no objects.
        """
        raise NotImplementedError()

    def obj_delete_single(self, request=None, **kwargs):
        """
        Deletes a single object.

        This needs to be implemented at the user level.

        `DocumentResource` includes a working version for MongoEngine
        `Documents`.
        """
        raise NotImplementedError()

    def obj_delete_list(self, request=None, **kwargs):
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

        if meta:
            # We may define a queryset or an object_class.
            if hasattr( meta, 'queryset' ) and not hasattr( meta, 'object_class' ):
                setattr( meta, 'object_class', meta.queryset._document )
            
            elif hasattr( meta, 'object_class' ) and not hasattr( meta, 'queryset' ):
                if hasattr( meta.object_class, 'objects' ):
                    setattr( meta, 'queryset', meta.object_class.objects )

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

    @classmethod
    def should_skip_field( cls, field ):
        """
        Given a MongoDB field, return if it should be included in the
        contributed ApiFields.
        """
        # Ignore reference fields for now, because objects know nothing about 
        # any API through which they're exposed. 
        if isinstance( field, mf.ReferenceField ):
            # The equivalent of ToOne
            return True

        if isinstance( field, mf.ListField ):
            if isinstance( field.field, ( mf.ReferenceField ) ):
                # The equivalent of ToMany ( many ToOne's )
                return True

        return False

    @classmethod
    def api_field_from_mongoengine_field( cls, f, default=fields.StringField ):
        """
        Returns the field type that would likely be associated with each
        MongoEngine type.

        """
        result = default  # instantiated only once by specifying it as kwarg

        # Specify only those field types that differ from default StringField
        if isinstance( f, mf.BooleanField ):
            result = fields.BooleanField
        elif isinstance( f, mf.FloatField ):
            result = fields.FloatField
        elif isinstance( f, mf.DecimalField ):
            result = fields.DecimalField
        elif isinstance( f, ( mf.IntField, mf.SequenceField ) ):
            result = fields.IntegerField
        elif isinstance( f, ( mf.FileField, mf.ImageField, mf.BinaryField ) ):
            result = fields.FileField
        elif isinstance( f, ( mf.DictField, mf.MapField ) ):
            result = fields.DictField
        elif isinstance( f, ( mf.DateTimeField, mf.ComplexDateTimeField ) ):
            result = fields.DateTimeField
        elif isinstance( f, ( mf.ListField, mf.SortedListField, mf.GeoPointField ) ):
            # This will be lists of simple objects, since references have been
            # discarded already by should_skip_fields. 
            result = fields.ListField
        elif isinstance( f, mf.ObjectIdField, ):
            result = fields.ObjectIdField

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

            # Exotic fields (currently Relational fields only) are filtered 
            # out by `should_skip_field`.
            if cls.should_skip_field( f ):
                continue

            api_field_class = cls.api_field_from_mongoengine_field( f )

            kwargs = {
                'attribute': f.name,
                'help_text': f.help_text,
            }

            if f.required is True:
                kwargs['required'] = True

            kwargs['unique'] = f.unique

            if f.default:
                kwargs['default'] = f.default

            if getattr( f, 'auto_now', False ):
                kwargs['default'] = f.auto_now

            if getattr( f, 'auto_now_add', False ):
                kwargs['default'] = f.auto_now_add

            final_fields[name] = api_field_class( **kwargs )
            final_fields[name].field_name = name

        return final_fields

    def dehydrate_id( self, bundle ):
        '''
        pk is present on objects, but not a MongoEngine field. Hence we need to
        explicitly dehydrate it since it won't be included in _fields.
        '''
        return bundle.obj.pk

    def get_resource_uri( self, request, bundle_or_object=None, absolute=None ):
        """
        Returns the resource's relative uri per the given API.

        *elements, if given, is used by Pyramid to specify instances 
        """
        kwargs = {
            'resource_name': self._meta.resource_name,
            'absolute': not not absolute if absolute else self._meta.use_absolute_uris,
        }

        if bundle_or_object:
            kwargs['operation'] = 'single'
            if isinstance( bundle_or_object, Bundle ):
                try:
                    kwargs['id'] = bundle_or_object.obj.pk
                except AttributeError:
                    # We may have received a DBRef, that doesn't have 'pk' but does have 'id'
                    try:
                        kwargs['id'] = bundle_or_object.obj.id
                    except AttributeError:
                        # No way to make up a uri.
                        raise NotImplementedError(' Could not find a pk or id for {0}'.format(bundle_or_object))
            else:
                kwargs['id'] = bundle_or_object.pk
        else:
            kwargs['operation'] = 'list'

        return self._meta.api.build_uri( request, **kwargs )

    def apply_sorting( self, obj_list, options=None ):
        """
        Given a dictionary of options, apply some ODM-level sorting to the
        provided `QuerySet`.

        Looks for the `sort_by` key and handles either ascending ( just the
        field name ) or descending ( the field name with a `-` in front ).
        """
        if options is None:
            options = {}

        parameter_name = 'sort_by'

        if not 'sort_by' in options:
            # Nothing to alter the sorting. Return what we've got.
            return obj_list

        sort_by_args = []

        if hasattr( options, 'getlist' ):
            sort_bits = options.getlist( parameter_name )
        else:
            sort_bits = options.get( parameter_name )

            if not isinstance( sort_bits, ( list, tuple )):
                sort_bits = [sort_bits]

        for sort_by in sort_bits:
            sort_by_bits = sort_by.split( LOOKUP_SEP )

            field_name = sort_by_bits[0]
            order = ''

            if sort_by_bits[0].startswith( '-' ):
                field_name = sort_by_bits[0][1:]
                order = '-'

            if not field_name in self.fields:
                # It's not a field we know about. Move along citizen.
                raise InvalidSortError( "No matching `{0}` field for ordering on.".format(field_name) )

            if not field_name in self._meta.ordering:
                raise InvalidSortError( "The `{0}` field does not allow ordering.".format(field_name) )

            if self.fields[field_name].attribute is None:
                raise InvalidSortError( "The `{0}` field has no 'attribute' for ordering with.".format(field_name) )

            sort_by_args.append( "{0}{1}".format( order, LOOKUP_SEP.join( [self.fields[field_name].attribute] + sort_by_bits[1:] ) ) )

        #FIXME: the mongo-specific part!
        return obj_list.sort_by( *sort_by_args )

    def build_filters( self, filters=None ):
        """
        Given a dictionary of filters, create the corresponding DRM filters,
        checking whether filtering on the field is allowed in the Resource
        definition.

        Valid values are either a list of MongoEngine filter types ( i.e.
        `['startswith', 'exact', 'lte']` ), the `ALL` constant or the
        `ALL_WITH_RELATIONS` constant.

        At the declarative level:
            filtering = {
                'resource_field_name': ['exact', 'startswith', 'endswith', 'contains'],
                'resource_field_name_2': ['exact', 'gt', 'gte', 'lt', 'lte', 'range'],
                'resource_field_name_3': ALL,
                'resource_field_name_4': ALL_WITH_RELATIONS,
                ...
            }

        Accepts the filters as a dict. None by default, meaning no filters.
        """
        if filters is None:
            filters = {}

        qs_filters = {}

        for filter_expr, value in filters.items():
            filter_bits = filter_expr.split( LOOKUP_SEP )
            field_name = filter_bits.pop( 0 )
            filter_type = 'exact'

            if not field_name in self.fields:
                # Not a field the Resource knows about, so ignore it.
                continue

            if len( filter_bits ) and filter_bits[-1] in QUERY_TERMS:
                filter_type = filter_bits.pop()

            lookup_bits = self.check_filtering( field_name, filter_type, filter_bits )
            value = self.filter_value_to_python( value, field_name, filters, filter_expr, filter_type )

            db_field_name = LOOKUP_SEP.join( lookup_bits )
            qs_filter = "{0}{1}{2}".format( db_field_name, LOOKUP_SEP, filter_type )
            qs_filters[qs_filter] = value

        return qs_filters

    def get_queryset( self, request ):
        if hasattr( self._meta, "queryset" ):
            return self._meta.queryset.clone()
        else:
            raise NotImplementedError('Resource needs a `queryset` to return objects')


    def track_changes( self, bundle ):
        # Find out if our no-longer-related documents still validate...
        if not hasattr(bundle, 'added_relations'):
            bundle.added_relations = set()
            bundle.removed_relations = set()

        changed_relations = bundle.obj.get_changed_relations()
        for c in changed_relations:
            added, removed = bundle.obj.get_changes_for_relation(c)
            bundle.added_relations |= added
            bundle.removed_relations |= removed

        # For good measure (shouldn't happen any more) 
        bundle.added_relations.discard(None)
        bundle.removed_relations.discard(None)

        return bundle

    def save_new( self, bundle ):
        '''
        Creates the object in the bundle if it doesn't have a primary key yet.
        Recurses for embedded related bundles.
        '''
        bundle.created = set()

        # STEP 1: If we're brand spankin' new try to get us an id.
        if not bundle.obj.pk:
            try:
                bundle = self.track_changes( bundle )
                bundle.obj.save( request=bundle.request, cascade=False ) 
                print('    ~~~~~ CREATED {2}: `{0}` (id={1})'.format(bundle.obj, bundle.obj.pk, type(bundle.obj)._class_name))
                bundle.data['resource_uri'] = self.get_resource_uri( bundle.request, bundle )
                bundle.created.add( bundle.data['resource_uri'] )
            except MongoEngineValidationError, e:
                # Ouch, that didn't work...
                pass


        # STEP 2: Create any nested related resources that are new (may recurse)
        for field_name, fld in self.fields.items():
            if getattr( fld, 'is_related', False ) and field_name in bundle.data: 
                related_data = bundle.data[ field_name ]
                if not related_data:
                    # This can happen if the field is not required and no data
                    # was given, so bundle.data[ field_name ] can be None or []
                    continue

                # The field has data in the form of a single or list of Bundles.
                # Delegate saving of new related objects to the related resource.
                related_resource = fld.get_related_resource()
                if getattr( fld, 'is_tomany', False ):
                    bundle.data[ field_name ] = [ related_resource.save_new( related_bundle ) for related_bundle in related_data ]
                    for related_bundle in bundle.data[ field_name ]:
                        bundle.created |= related_bundle.created
                else:
                    bundle.data[ field_name ] = related_resource.save_new( related_data )
                    bundle.created |= bundle.data[ field_name ].created


        # STEP 3: Every member should have received an id now, so we should be
        # able to save ourself unless something really unexpected happened.
        if not bundle.obj.pk:
            try:
                bundle = self.track_changes( bundle )
                bundle.obj.save( request=bundle.request, cascade=False ) 
                print('    ~~~~~ CREATED {2}: `{0}` (id={1})'.format(bundle.obj, bundle.obj.pk, type(bundle.obj)._class_name))
                bundle.data['resource_uri'] = self.get_resource_uri( bundle.request, bundle )
                bundle.created.add( bundle.data['resource_uri'] )
            except Exception, e:
                # Something went wrong. Test more specific exceptions later.
                # For now, roll back any objects we created along the way.
                # FIXME: this is more involved since created objects trigger
                # relational and privilege updates. Use `self.obj_delete_single` after
                # we've fleshed that out.
                #for doc in bundle.created:
                #    doc.delete( request=bundle.request )
                raise 

        return bundle

    def validate( self, bundle ):
        '''
        Recursively validates the (embedded) object(s) in the bundle.

        Call validate on every related object and then validate ourselves.
        If validation fails, roll back by deleting objects that were created
        and raise a ValidationError.
        '''

        # STEP 4: All objects now exist and all relations are assigned, so
        # everything should validate. 
        if not isinstance( bundle, Bundle):
            return bundle

        #assert isinstance( bundle, Bundle )

        try:
            bundle.obj.validate( request=bundle.request )
        except MongoEngineValidationError, e:
            bundle.errors['ValidationError'] = e

        if not bundle.errors:
            for field_name, fld in self.fields.items():
                if getattr( fld, 'is_related', False ) and field_name in bundle.data:
                    related_data = bundle.data[ field_name ]
                    if not related_data:
                        # This can happen if the field is not required and no data
                        # was given, so bundle.data[ field_name ] can be None or []
                        continue

                    # Have the related resource validate its document(s) in turn.
                    related_resource = fld.get_related_resource()
                    if getattr( fld, 'is_tomany', False ):
                        bundle.data[ field_name ] = [ related_resource.validate( related_bundle ) for related_bundle in related_data ]
                        errors = [ related_bundle.errors for related_bundle in bundle.data[ field_name ] if related_bundle.errors ]
                        if errors:
                            bundle.errors[ field_name ] = errors
                    else:
                        bundle.data[ field_name ] = related_resource.validate( bundle.data[ field_name ] )
                        if bundle.data[ field_name ].errors:
                            bundle.errors[ field_name ] = bundle.data[ field_name ].errors

        if bundle.errors:
            # Validation failed along the way. Delete any created documents.
            # FIXME: this is more involved since created objects trigger
            # relational and privilege updates. Use `self.obj_delete_single` after
            # we've fleshed that out.
            #for doc in bundle.created:
            #    doc.delete( request=bundle.request )
            raise ValidationError('Errors were encountered validating the document: {0}'.format(bundle.errors))

        return bundle

    def update( self, bundle ):
        '''
        Recursively saves the (embedded) object(s) in the validated bundle.

        5. This is wicked! We have a totally valid bundle!
           Try to save ourselves again, recursing for related resources.

           IMPORTANT NOTE: 
           We cannot assume that objects for which we only provided a URI 
           have not changed: they likely have received updates from the other
           side of their relations (if you use RelationalMixin), or have 
           updated privileges (if you use PrivilegeMixin)

           MongoEngine will take care of not updating fields that haven't changed. 

           We do a bit of accounting as an additional check: if anything goes 
           amiss here you get the ids of objects that were and weren't saved,
           which you can use to create your own rollback scenario.
        '''

        # Save the object in the bundle. 
        bundle = self.track_changes( bundle )
        bundle.obj.save( request=bundle.request, cascade=False )
        print('    ~~~~~ UPDATED `{2}`: `{0}` (id={1})'.format(bundle.obj, bundle.obj.pk, type(bundle.obj)._class_name))

        for field_name, fld in self.fields.items():
            if getattr( fld, 'is_related', False ) and field_name in bundle.data: 
                related_data = bundle.data[ field_name ]
                if not related_data:
                    # This can happen if the field is not required and no data
                    # was given, so bundle.data[ field_name ] can be None or []
                    continue

                # The field has data in the form of one or a list of Bundles.
                # Delegate updating new and former relations to the related resource.
                related_resource = fld.get_related_resource()
                if getattr( fld, 'is_tomany', False ):

                    updated_data = []
                    for related_bundle in related_data:
                        # Only update when the relation has actually changed.
                        if related_bundle.from_data or (related_bundle.from_uri and related_bundle.obj in bundle.added_relations):
                            related_bundle = related_resource.update( related_bundle )
                        updated_data.append(related_bundle)

                    bundle.data[ field_name ] = updated_data

                elif related_data.from_data or (related_data.from_uri and related_data.obj in bundle.added_relations):
                    # Related data contains a bundle for a single related resource
                    bundle.data[ field_name ] = related_resource.update( related_data )

                # When objects are removed, at least their reverse relational 
                # data and likely their privileges have changed. Since they're 
                # not present in the bundle tree, we need to save them here.
                for obj in bundle.removed_relations:
                    obj.save( request=bundle.request )

        return bundle


    def obj_get_list( self, request=None, **kwargs ):
        """
        A Pyramid/MongoEngine implementation of `obj_get_list`.
        """
        filters = {}
        if request and hasattr( request, 'GET' ):
            # Pyramid's Request object uses a Multidict for its representation.
            # Transform this into an 'ordinary' dict for further processing.
            filters = request.GET.mixed()

        # Update with the provided kwargs.
        filters.update( kwargs )
        applicable_filters = self.build_filters( filters=filters )

        try:
            return self.get_queryset( request ).filter( **applicable_filters )
        except ValueError:
            raise BadRequest( "Invalid resource lookup data provided ( mismatched type )." )

    def obj_get_single( self, request=None, **kwargs ):
        """
        A Pyramid/MongoEngine specific implementation of `obj_get_single`.

        Uses `obj_get_list` to get the initial list, which should contain
        only one instance matched by the request and the provided `kwargs`.
        """
        uri = kwargs.pop('uri', None)
        if uri:
            # Grab the pk from the uri and create a filter from it.
            # FIXME: Assumption! Should use an API function to get the resource.
            kwargs['pk'] = uri.split('/')[-2]

        obj_list = self.obj_get_list( request ).filter( **kwargs )
        # `get_queryset` should return only 1 object with the provided
        # kwargs. However if the kwargs are off, it could return none, or
        # multiple objects. Find out if we matched only 1 and be smart 
        # about queries: every len() causes one.
        object = None
        for obj in obj_list:
            if object:
                # We've already set object the first run, so we shouldn't 
                # get here unless there's more than one object at 
                # this (possibly filtered) URI
                stringified_kwargs = ', '.join( ["{0}={1}".format( k, v ) for k, v in kwargs.items()] )
                raise self._meta.object_class.MultipleObjectsReturned( "More than one `{0}` matched `{1}`.".format( self._meta.object_class.__name__, stringified_kwargs ) )
            object = obj

        if object is None:
            # If we didn't find an object the filter parameters were off
            stringified_kwargs = ', '.join( ["{0}={1}".format( k, v ) for k, v in kwargs.items()] )
            raise self._meta.object_class.DoesNotExist( "Couldn't find an instance of `{0}` which matched `{1}`.".format( self._meta.object_class.__name__, stringified_kwargs ) )

        # Okay, we're good to go without superfluous queries!
        return object

    def obj_delete_single( self, request=None, **kwargs ):
        """
        Tries to retrieve a resource per the given `request` and `kwargs` and
        deletes it if found. 

        Returns `HTTPNoContent` if successful, of `HTTPNotFound`.
        """
        try:
            obj = self.obj_get_single(request, **kwargs)
        except DoesNotExist:
            raise NotFound("A model instance matching the provided arguments could not be found.")

        # We must first notify our relations that we're going to be removed.
        obj.clear_relations()

        # Find out if our no-longer-related documents still validate...
        changed_documents = set()
        changed_relations = obj.get_changed_relations()
        for c in changed_relations:
            added, removed = obj.get_changes_for_relation(c)
            changed_documents |= added
            changed_documents |= removed

        # For good measure (shouldn't happen any more) 
        changed_documents.discard(None)

        for doc in changed_documents:
            try: 
                doc.validate( request=request )
            except MongoEngineValidationError, e:
                raise ValidationError('Deletion of `{0}` prohibited since it would invalidate some relations')

        # All clear. 
        for doc in changed_documents:
            doc.save( request=request )

        # We should no longer have dangling relations: remove ourself.
        obj.delete( request=request )


class PrivilegedDocumentResource( DocumentResource ):
    """
    A special DocumentResource that inserts Privileges from the Document
    """
    def __init__( self, api=None ):
        from mongoengine_privileges import PrivilegeMixin

        # Make sure our corresponding Document actually has privileges
        if not issubclass( self._meta.object_class, PrivilegeMixin ):
            raise ConfigurationError( 'Document must also inherit from PrivilegeMixin' )
        super( PrivilegedDocumentResource, self ).__init__( api=api )

    # The privileges field will be present on all our resources
    privileges = fields.ListField(
        default = [],
        readonly = True
    )

    def get_queryset( self, request ):
        return self._meta.object_class.objects( __raw__={ '$or': [
            { 'privileges.user.$id' : request.user.pk, 'privileges.permissions': 'read' },
            { 'privileges.user.groups': { '$in': request.user.groups }, 'privileges.permissions': 'read' }
        ]})

    def dehydrate_privileges( self, bundle ):
        priv = bundle.obj.get_privilege( bundle.request.user )
        return priv.permissions if priv else []


