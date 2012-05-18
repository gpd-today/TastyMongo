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
from .authorization import ReadOnlyAuthorization
from .throttle import BaseThrottle
from .validation import Validation

from pyramid.response import Response
from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned
import mongoengine.document 
import mongoengine.fields as mf

from copy import deepcopy

class ResourceOptions( object ):
    """
    A configuration class for ``Resource``.

    Provides sane defaults and the logic needed to augment these settings with
    the internal ``class Meta`` used on ``Resource`` subclasses.
    """
    serializer = Serializer()
    authentication = Authentication()
    authorization = ReadOnlyAuthorization()
    throttle = BaseThrottle()
    validation = Validation()
    allowed_methods = [ 'get', 'post', 'put', 'delete' ]
    list_allowed_methods = None
    detail_allowed_methods = None
    limit = 20
    api = None
    resource_name = None
    default_format = 'application/json'
    filtering = {}
    ordering = []
    object_class = None
    queryset = None
    fields = []
    excludes = []
    include_resource_uri = True
    include_resource_url = False

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

        if overrides.get( 'detail_allowed_methods', None ) is None:
            overrides['detail_allowed_methods'] = allowed_methods

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

                for field_name, field_object in parent_class_fields.items():
                    attrs['base_fields'][field_name] = deepcopy( field_object )
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
            # No ``resource_name`` provided. Attempt to auto-name the resource.
            class_name = new_class.__name__
            name_bits = [bit for bit in class_name.split( 'Resource' ) if bit]
            resource_name = ''.join( name_bits ).lower()
            new_class._meta.resource_name = resource_name

        if getattr( new_class._meta, 'include_resource_uri', True ):
            if not 'resource_uri' in new_class.base_fields:
                new_class.base_fields['resource_uri'] = fields.StringField( readonly=True )
        elif 'resource_uri' in new_class.base_fields and not 'resource_uri' in attrs:
            del( new_class.base_fields['resource_uri'] )

        if getattr( new_class._meta, 'include_resource_url', False ):
            if not 'resource_url' in new_class.base_fields:
                new_class.base_fields['resource_url'] = fields.StringField( readonly=True )
        elif 'resource_url' in new_class.base_fields and not 'resource_url' in attrs:
            del( new_class.base_fields['resource_url'] )

        for field_name, field_object in new_class.base_fields.items():
            if hasattr( field_object, 'contribute_to_class' ):
                field_object.contribute_to_class( new_class, field_name )

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

    def determine_format( self, request ):
        """
        Used to determine the desired format.

        Largely relies on ``tastypie.utils.mime.determine_format`` but here
        as a point of extension.
        """
        return determine_format( request, self._meta.serializer, default_format=self._meta.default_format )

    def dispatch( self, request_type, request, **kwargs ):
        """
        Handles the common operations ( allowed HTTP method, authentication,
        throttling, method lookup ) surrounding most CRUD interactions.
        """
        allowed_methods = getattr( self._meta, '%s_allowed_methods' % request_type, None )
        request_method = self.check_method( request, allowed=allowed_methods )
        print( 'resource={}; request={}_{}'.format( self._meta.resource_name, request_method, request_type ))

        # Determine which callback we're going to use
        method = getattr( self, '{}_{}'.format( request_method, request_type ), None )

        if method is None:
            detail = 'Method="{}_{}" is not implemented for resource="{}"'.format( request_method, request_type, self._meta.resource_name )
            raise ImmediateHTTPResponse( response=http.HTTPNotImplemented( body=detail ))

        self.is_authenticated( request )
        self.check_throttle( request )

        # All clear. Process the request.
        response = method( request, **kwargs )

        # Add the throttled request.
        self.log_throttled_access(request)

        return response

    def dispatch_list( self, request, **kwargs ):
        """
        A view for handling the various HTTP methods ( GET/POST/PUT/DELETE ) over
        the entire list of resources.
        
        Relies on ``Resource.dispatch`` for the heavy-lifting.
        """
        return self.dispatch( 'list', request, **kwargs )

    def dispatch_detail( self, request, **kwargs ):
        """
        A view for handling the various HTTP methods ( GET/POST/PUT/DELETE ) on
        a single resource.

        Relies on ``Resource.dispatch`` for the heavy-lifting.
        """
        return self.dispatch( 'detail', request, **kwargs )

    def log_throttled_access(self, request):
        """
        Handles the recording of the user's access for throttling purposes.

        Mostly a hook, this uses class assigned to ``throttle`` from
        ``Resource._meta``.
        """
        request_method = request.method.lower()
        self._meta.throttle.accessed( self._meta.authentication.get_identifier(request), url=request.path_url, request_method=request_method )

    def get_schema( self, request ):
        """
        Returns a serialized form of the schema of the resource.

        Calls ``build_schema`` to generate the data. This method only responds
        to HTTP GET.

        Should return a HTTPResponse ( 200 OK ).
        """
        self.check_method( request, allowed=['get'] )
        self.is_authenticated( request )
        self.check_throttle( request )
        self.log_throttled_access(request)
        return self.create_response( request, self.build_schema() )

    def get_list( self, request ):
        """
        Returns a serialized list of resources.

        Calls ``obj_get_list`` to provide the data, then handles that result
        set and serializes it.

        Should return a HTTPResponse ( 200 OK ).
        """
        objects = self.obj_get_list( request=request, **request.matchdict )
        sorted_objects = self.apply_sorting( objects, options=request.GET )

        to_be_serialized = { 'meta': 'get_list', 'resource_uri': self.get_resource_uri( request ), 'objects': sorted_objects, }

        # Dehydrate the bundles in preparation for serialization.
        bundles = [self.build_bundle( obj=obj, request=request ) for obj in to_be_serialized['objects']]
        to_be_serialized['objects'] = [self.full_dehydrate( bundle ) for bundle in bundles]
        to_be_serialized = self.alter_list_data_to_serialize( request, to_be_serialized )
        return self.create_response( request, to_be_serialized )

    def get_detail( self, request ):
        """
        Returns a single serialized resource.

        Should return a HTTPResponse ( 200 OK ).
        """
        try:
            obj = self.obj_get( request=request, **request.matchdict )
        except DoesNotExist:
            return http.HTTPNotFound()
        except MultipleObjectsReturned:
            return http.HTTPMultipleChoices( "More than one resource is found at this URI." )

        # try to figure out how to get these related resources
        bundle = self.build_bundle( obj=obj, request=request )
        bundle = self.full_dehydrate( bundle )
        bundle = self.alter_detail_data_to_serialize( request, bundle )
        return self.create_response( request, bundle )

    def post_list(self, request, **kwargs):
        """
        Creates a new resource/object with the provided data.

        Calls ``obj_create`` with the provided data and returns a response
        with the new resource's location.

        If a new resource is created, return ``HttpCreated`` (201 Created).
        If ``Meta.always_return_data = True``, there will be a populated body
        of serialized data.
        """
        deserialized = self.deserialize(request, request.body, format=request.content_type)
        deserialized = self.alter_deserialized_detail_data(request, deserialized)
        bundle = self.build_bundle(data=deserialized, request=request)
        updated_bundle = self.obj_create(bundle, request=request, **kwargs)
        location = self.get_resource_uri(updated_bundle)

        if not self._meta.always_return_data:
            return http.HttpCreated(location=location)
        else:
            updated_bundle = self.full_dehydrate(updated_bundle)
            updated_bundle = self.alter_detail_data_to_serialize(request, updated_bundle)
            return self.create_response(request, updated_bundle, response_class=http.HttpCreated, location=location)

    def post_detail(self, request, **kwargs):
        """
        Not implemented since we don't allow self-referential nested URLs
        """
        return http.HttpNotImplemented()
    
    def delete_list(self, request, **kwargs):
        """
        Not implemented since we don't allow destroying whole lists
        """
        return http.HttpNotImplemented()

    def delete_detail(self, request, **kwargs):
        """
        Destroys a single resource/object.

        Calls ``obj_delete``.

        If the resource is deleted, return ``HttpNoContent`` (204 No Content).
        If the resource did not exist, return ``Http404`` (404 Not Found).
        """
        try:
            self.obj_delete(request=request, **kwargs)
            return http.HttpNoContent()
        except NotFound:
            return http.HttpNotFound()

    def check_method( self, request, allowed=None ):
        """
        Ensures that the HTTP method used on the request is allowed to be
        handled by the resource.
        
        Takes an ``allowed`` parameter, which should be a list of lowercase
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
            response = http.HTTPMethodNotAllowed( body='Allowed methods={}'.format( allows ))
            raise ImmediateHTTPResponse( response=response )

        return request_method

    def check_filtering( self, field_name, filter_type='exact', filter_bits=None ):
        """
        Given a field name, a optional filter type and an optional list of
        additional relations, determine if a field can be filtered on.

        If a filter does not meet the needed conditions, it should raise an
        ``InvalidFilterError``.

        If the filter meets the conditions, a list of attribute names ( not
        field names ) will be returned.
        """
        if filter_bits is None:
            filter_bits = []

        if not field_name in self._meta.filtering:
            raise InvalidFilterError( "The '%s' field does not allow filtering." % field_name )

        # Check to see if it's an allowed lookup type.
        if not self._meta.filtering[field_name] in ( ALL, ALL_WITH_RELATIONS ):
            # Must be an explicit whitelist.
            if not filter_type in self._meta.filtering[field_name]:
                raise InvalidFilterError( "'%s' is not an allowed filter on the '%s' field." % ( filter_type, field_name ))

        if self.fields[field_name].attribute is None:
            raise InvalidFilterError( "The '%s' field has no 'attribute' for searching with." % field_name )

        # Check to see if it's a relational lookup and if that's allowed.
        if len( filter_bits ):
            if not getattr( self.fields[field_name], 'is_related', False ):
                raise InvalidFilterError( "The '%s' field does not support relations." % field_name )

            if not self._meta.filtering[field_name] == ALL_WITH_RELATIONS:
                raise InvalidFilterError( "Lookups are not allowed more than one level deep on the '%s' field." % field_name )

            # Recursively descend through the remaining lookups in the filter,
            # if any. We should ensure that all along the way, we're allowed
            # to filter on that field by the related resource.
            related_resource = self.fields[field_name].get_related_resource( None )
            return [self.fields[field_name].attribute] + related_resource.check_filtering( filter_bits[0], filter_type, filter_bits[1:] )

        return [self.fields[field_name].attribute]

    def filter_value_to_python( self, value, field_name, filters, filter_expr, filter_type ):
        """
        Turn the string ``value`` into a python object.
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

    def is_authenticated( self, request ):
        """
        Handles checking if the user is authenticated and dealing with
        unauthenticated users.

        Mostly a hook, this uses class assigned to ``authentication`` from
        ``Resource._meta``.
        """
        # Authenticate the request as needed.
        auth_result = self._meta.authentication.is_authenticated( request )

        if isinstance( auth_result, Response ):
            raise ImmediateHTTPResponse( response=auth_result )

        if not auth_result is True:
            raise ImmediateHTTPResponse( response=http.HTTPUnauthorized() )

    def is_valid(self, bundle, request=None):
        """
        Handles checking if the data provided by the user is valid.

        Mostly a hook, this uses class assigned to ``validation`` from
        ``Resource._meta``.

        If validation fails, an error is raised with the error messages
        serialized inside it.
        """
        errors = self._meta.validation.is_valid(bundle, request)

        if errors:
            bundle.errors[self._meta.resource_name] = errors
            return False

        return True

    def check_throttle( self, request ):
        """
        Handles checking if the user should be throttled.

        Mostly a hook, this uses class assigned to ``throttle`` from
        ``Resource._meta``.
        """
        identifier = self._meta.authentication.get_identifier( request )

        # Check to see if they should be throttled.
        if self._meta.throttle.should_be_throttled( identifier ):
            # Throttle limit exceeded.
            raise ImmediateHTTPResponse( response=http.HTTPForbidden() )

    def create_response( self, request, data, response_class=Response, **response_kwargs ):
        """
        Extracts the common "which-format/serialize/return-response" cycle.

        Mostly a useful shortcut/hook.
        """
        desired_format = self.determine_format( request )
        serialized = self.serialize( request, data, desired_format )
        return response_class( body=serialized, content_type=build_content_type( desired_format ), **response_kwargs )

    def error_response( self, errors, request ):
        if request:
            desired_format = self.determine_format( request )
        else:
            desired_format = self._meta.default_format

        serialized = self.serialize( request, errors, desired_format )
        response = http.HTTPBadRequest( body=serialized, content_type=build_content_type( desired_format ))
        raise ImmediateHTTPResponse( response=response )

    def serialize( self, request, data, format, options=None ):
        """
        Given a request, data and a desired format, produces a serialized
        version suitable for transfer over the wire.

        Mostly a hook, this uses the ``Serializer`` from ``Resource._meta``.
        """
        return self._meta.serializer.serialize( data, format, options )

    def alter_list_data_to_serialize( self, request, data ):
        """
        A hook to alter list data just before it gets serialized & sent to the user.

        Useful for restructuring/renaming aspects of the what's going to be
        sent.

        Should accommodate for a list of objects, generally also including
        meta data.
        """
        return data

    def alter_detail_data_to_serialize( self, request, data ):
        """
        A hook to alter detail data just before it gets serialized & sent to the user.

        Useful for restructuring/renaming aspects of the what's going to be
        sent.

        Should accommodate for receiving a single bundle of data.
        """
        return data
    
    def alter_deserialized_list_data( self, request, data ):
        """
        A hook to alter list data just after it has been received from the user &
        gets deserialized.

        Useful for altering the user data before any hydration is applied.
        """
        return data

    def alter_deserialized_detail_data( self, request, data ):
        """
        A hook to alter detail data just after it has been received from the user &
        gets deserialized.

        Useful for altering the user data before any hydration is applied.
        """
        return data

    def deserialize( self, request, data, format=None ):
        """
        Given a request, data and a format, deserializes the given data.

        It relies on the request properly sending a ``CONTENT_TYPE`` header,
        falling back to the default format if not provided.

        Mostly a hook, this uses the ``Serializer`` from ``Resource._meta``.
        """
        format = format or request.content_type or self._meta.default_format
        return self._meta.serializer.deserialize( data, format )

    def build_bundle( self, obj=None, data=None, request=None ):
        """
        Given either an object, a data dictionary or both, builds a ``Bundle``
        for use throughout the ``dehydrate/hydrate`` cycle.

        If no object is provided, an empty object from
        ``Resource._meta.object_class`` is created so that attempts to access
        ``bundle.obj`` do not fail.
        """
        if obj is None:
            obj = self._meta.object_class()

        b = Bundle( obj=obj, data=data, request=request )
        return b

    def get_resource_uri( self, request, bundle_or_obj = None ):
        """
        This is the `relative` uri of the object
        """
        raise NotImplementedError()

    def get_resource_url( self, request, bundle_or_obj = None ):
        """
        This is the `absolute` uri of the object
        """
        raise NotImplementedError()

    def hydrate(self, bundle):
        """
        Given a populated bundle, distill it and turn it back into
        a full-fledged object instance.
        """
        if bundle.obj is None:
            bundle.obj = self._meta.object_class()

        bundle = self.hydrate(bundle)
        for field_name, field_object in self.fields.items():
            if field_object.readonly is True:
                continue

            # Check for an optional field_specific method on the resource 
            # to do any further hydration for the field before calling the
            # field's own hydrate method.
            method = getattr(self, "hydrate_%s" % field_name, None)
            if method:
                bundle = method(bundle)

            if field_object.attribute:
                # Use the field's hydrate method to obtain a possibly complex
                # value from its corresponding simple deserialized json data
                value = field_object.hydrate(bundle)

                if value is not None or not field_object.required:
                    # For non-related fields, set `value` to the corresponding
                    # document attribute and get on with it.
                    if not getattr(field_object, 'is_related', False):
                        setattr(bundle.obj, field_object.attribute, value)
                    else:
                        # We're related to another /resource/.  

                    elif not getattr(field_object, 'is_m2m', False):
                        if isinstance(value, Bundle) and value.errors.get(field_name):
                            # copy errors from our hydrated field to the main bundle
                            bundle.errors[field_name] = value.errors[field_name]

                        if value is not None:
                            setattr(bundle.obj, field_object.attribute, value.obj)
                        elif field_object.blank:
                            continue
                        elif field_object.null:
                            setattr(bundle.obj, field_object.attribute, value)

        return bundle

    def full_dehydrate( self, bundle ):
        """
        Given a bundle with an object instance, extract the information from it
        to populate the resource.
        """
        # Dehydrate each field.
        for field_name, field_object in self.fields.items():
            bundle.data[field_name] = field_object.dehydrate( bundle )

            # Check for an optional method to do further dehydration.
            method = getattr( self, "dehydrate_%s" % field_name, None )

            if method:
                bundle.data[field_name] = method( bundle.request, bundle )

        bundle = self.dehydrate( bundle )
        return bundle

    def dehydrate( self, bundle ):
        """
        A hook to allow a final manipulation of data once all fields/methods
        have built out the dehydrated data.

        Useful if you need to access more than one dehydrated field or want
        to annotate on additional data.

        Must return the modified bundle.
        """
        return bundle

    def dehydrate_resource_uri( self, request, bundle ):
        """
        For the automatically included ``resource_uri`` field, dehydrate
        the URI for the given bundle.
        """
        try:
            return self.get_resource_uri( request, bundle )
        except NotImplementedError:
            return '<not implemented>'

    def dehydrate_resource_url( self, request, bundle ):
        """
        For the automatically included ``resource_url`` field, dehydrate
        the URL for the given bundle.
        """
        if not self._meta.include_resource_uri:
            return ''

        try:
            return self.get_resource_url( request, bundle )
        except NotImplementedError:
            return '<not implemented>'

    def obj_get( self, request=None, **kwargs ):
        """
        Fetches an individual object on the resource.

        This needs to be implemented at the user level. If the object can not
        be found, this should raise a ``NotFound`` exception.
        """
        raise NotImplementedError()

    def obj_get_list( self, request=None, **kwargs ):
        """
        Fetches the list of objects available on the resource.

        This needs to be implemented at the user level.
        """
        raise NotImplementedError()

    def obj_create(self, bundle, request=None, **kwargs):
        """
        Creates a new object based on the provided data.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def obj_update(self, bundle, request=None, **kwargs):
        """
        Updates an existing object (or creates a new object) based on the
        provided data.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def obj_delete_list(self, request=None, **kwargs):
        """
        Deletes an entire list of objects.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def obj_delete(self, request=None, **kwargs):
        """
        Deletes a single object.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def build_schema( self ):
        """
        Returns a dictionary of all the fields on the resource and some
        properties about those fields.

        Used by the ``schema/`` endpoint to describe what will be available.
        """
        data = {
            'fields': {},
            'default_format': self._meta.default_format,
            'allowed_list_http_methods': self._meta.list_allowed_methods,
            'allowed_detail_http_methods': self._meta.detail_allowed_methods,
            'default_limit': self._meta.limit,
        }

        if self._meta.ordering:
            data['ordering'] = self._meta.ordering

        if self._meta.filtering:
            data['filtering'] = self._meta.filtering

        for field_name, field_object in self.fields.items():
            data['fields'][field_name] = {
                'default': field_object.default,
                'type': field_object.dehydrated_type,
                'required': field_object.required,
                'readonly': field_object.readonly,
                'help_text': field_object.help_text,
                'unique': field_object.unique,
            }
        return data
    
    def apply_sorting( self, obj_list, options=None ):
        """
        Given a dictionary of options, apply some ODM-level sorting to the
        provided ``QuerySet``.

        Looks for the ``sort_by`` key and handles either ascending ( just the
        field name ) or descending ( the field name with a ``-`` in front ).
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
                raise InvalidSortError( "No matching '%s' field for ordering on." % field_name )

            if not field_name in self._meta.ordering:
                raise InvalidSortError( "The '%s' field does not allow ordering." % field_name )

            if self.fields[field_name].attribute is None:
                raise InvalidSortError( "The '%s' field has no 'attribute' for ordering with." % field_name )

            sort_by_args.append( "%s%s" % ( order, LOOKUP_SEP.join( [self.fields[field_name].attribute] + sort_by_bits[1:] )) )

        #FIXME: the mongo-specific part!
        return obj_list.sort_by( *sort_by_args )


class DocumentDeclarativeMetaclass( DeclarativeMetaclass ):

    # Subclassed to handle specifics for MongoEngine Documents
    def __new__( cls, name, bases, attrs ):
        meta = attrs.get( 'Meta' )

        if meta:
            if hasattr( meta, 'queryset' ) and not hasattr( meta, 'object_class' ):
                setattr( meta, 'object_class', meta.queryset._document )
            
            if hasattr( meta, 'object_class' ) and not hasattr( meta, 'queryset' ):
                if hasattr( meta.object_class, 'objects' ):
                    setattr( meta, 'queryset', meta.object_class.objects )

        new_class = super( DocumentDeclarativeMetaclass, cls ).__new__( cls, name, bases, attrs )
        include_fields = getattr( new_class._meta, 'fields', [] )
        excludes = getattr( new_class._meta, 'excludes', [] )
        field_names = new_class.base_fields.keys()

        for field_name in field_names:
            if field_name in ( 'resource_uri', 'resource_url' ):
                # Embedded documents don't have their own resource_uri
                if meta and hasattr( meta, 'object_class' ) and issubclass( meta.object_class, mongoengine.EmbeddedDocument ):
                    del( new_class.base_fields[field_name] )
                continue
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
        # Ignore reference fields for now, because documents know nothing about 
        # any API through which they're exposed. 
        if isinstance( field, ( mf.ReferenceField ) ):
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
        result = default
        field_type = type( f )

        # Specify only those field types that differ from StringField
        if field_type in ( mf.BooleanField, ):
            result = fields.BooleanField
        elif field_type in ( mf.FloatField, ):
            result = fields.FloatField
        elif field_type in ( mf.DecimalField, ):
            result = fields.DecimalField
        elif field_type in ( mf.IntField, mf.SequenceField ):
            result = fields.IntegerField
        elif field_type in ( mf.FileField, mf.ImageField, mf.BinaryField ):
            result = fields.FileField
        elif field_type in ( mf.DictField, mf.MapField ):
            result = fields.DictField
        elif field_type in ( mf.DateTimeField, mf.ComplexDateTimeField ):
            result = fields.DateTimeField
        elif field_type in ( mf.ListField, mf.SortedListField, mf.GeoPointField ):
            # This will be lists of simple objects, since references have been
            # discarded already by should_skip_fields. 
            result = fields.ListField
        elif field_type in ( mf.ObjectIdField, ):
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

            if type( f ) == mf.StringField:
                kwargs['default'] = ''

            if f.default:
                kwargs['default'] = f.default

            if getattr( f, 'auto_now', False ):
                kwargs['default'] = f.auto_now

            if getattr( f, 'auto_now_add', False ):
                kwargs['default'] = f.auto_now_add

            final_fields[name] = api_field_class( **kwargs )
            final_fields[name].instance_name = name

        return final_fields

    def dehydrate_id( self, request, bundle ):
        '''
        id is present on objects, but not a MongoEngine field. Hence we need to
        explicitly dehydrate it since it won't be included in _fields.
        '''
        return bundle.obj.id

    def get_resource_uri( self, request, bundle_or_obj=None, absolute=False ):
        """
        Returns the resource's relative uri per the given API.

        *elements, if given, is used by Pyramid to specify instances 
        """
        kwargs = {
            'resource_name': self._meta.resource_name,
            'absolute': not not absolute,
        }

        if bundle_or_obj:
            kwargs['operation'] = 'detail'
            if isinstance( bundle_or_obj, Bundle ):
                try:
                    kwargs['id'] = bundle_or_obj.obj.id
                except AttributeError:
                    raise NotImplementedError()
            else:
                kwargs['id'] = bundle_or_obj.id
        else:
            kwargs['operation'] = 'list'

        return self._meta.api.build_uri( request, **kwargs )

    def get_resource_url( self, request, bundle_or_obj=None ):
        """
        Returns the resource's absolute uri per the given API.

        *elements, if given, is used by Pyramid to specify instances 
        """
        return self.get_resource_uri( request, bundle_or_obj, absolute=True )

    def build_filters( self, filters=None ):
        """
        Given a dictionary of filters, create the corresponding DRM filters,
        checking whether filtering on the field is allowed in the Resource
        definition.

        Valid values are either a list of MongoEngine filter types ( i.e.
        ``['startswith', 'exact', 'lte']`` ), the ``ALL`` constant or the
        ``ALL_WITH_RELATIONS`` constant.

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
            qs_filter = "%s%s%s" % ( db_field_name, LOOKUP_SEP, filter_type )
            qs_filters[qs_filter] = value

        return qs_filters

    def apply_filters( self, request, applicable_filters ):
        """
        A MongoEngine-specific implementation of ``apply_filters``.
        """
        return self.get_object_list( request ).filter( **applicable_filters )

    def get_object_list( self, request ):
        if hasattr( self._meta, "queryset" ):
            return self._meta.queryset.clone()
        else:
            raise NotImplementedError()

    def obj_get( self, request=None, **kwargs ):
        """
        A MongoEngine implementation of ``obj_get``.

        Takes optional ``kwargs``, which are used to narrow the query to find
        the instance.
        """
        try:
            object_list = self.get_object_list( request ).filter( **kwargs )
            obj = None

            # Be smart about queries: every len() causes one.
            for o in object_list:
                if obj:
                    # We've already set obj, so shouldn't get here unless
                    # there's more than one object at this (possibly filtered) URI
                    stringified_kwargs = ', '.join( ["%s=%s" % ( k, v ) for k, v in kwargs.items()] )
                    raise self._meta.object_class.MultipleObjectsReturned( "More than '%s' matched '%s'." % ( self._meta.object_class.__name__, stringified_kwargs ))
                obj = o

            if obj is None:
                # We should have exactly 1 object at this point
                stringified_kwargs = ', '.join( ["%s=%s" % ( k, v ) for k, v in kwargs.items()] )
                raise self._meta.object_class.DoesNotExist( "Couldn't find an instance of '%s' which matched '%s'." % ( self._meta.object_class.__name__, stringified_kwargs ))

            # Okay, we're good to go without superfluous queries
            return obj

        except ValueError:
            raise NotFound( "Invalid resource lookup data provided ( mismatched type )." )

    def obj_get_list( self, request=None, **kwargs ):
        """
        A Pyramid/MongoEngine implementation of ``obj_get_list``.
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
            return self.apply_filters( request, applicable_filters )
        except ValueError:
            raise BadRequest( "Invalid resource lookup data provided ( mismatched type )." )

    def obj_create(self, bundle, request=None, **kwargs):
        """
        A MongoEngine-specific implementation of ``obj_create``.
        """

        # Create an object of the right type
        bundle.obj = self._meta.object_class()

        for key, value in kwargs.items():
            setattr(bundle.obj, key, value)

        bundle = self.full_hydrate(bundle)
        self.is_valid(bundle,request)

        if bundle.errors:
            self.error_response(bundle.errors, request)

        # Save FKs just in case.
        self.save_related(bundle)

        # Save parent
        bundle.obj.save()

        # Now pick up the M2M bits.
        m2m_bundle = self.hydrate_m2m(bundle)
        self.save_m2m(m2m_bundle)
        return bundle

    def obj_update(self, bundle, request=None, skip_errors=False, **kwargs):
        """
        A DRM-specific implementation of ``obj_update``.
        """
        if not bundle.obj or not bundle.obj.pk:
            # Attempt to hydrate data from kwargs before doing a lookup for the object.
            # This step is needed so certain values (like datetime) will pass model validation.
            try:
                bundle.obj = self.get_object_list(bundle.request).model()
                bundle.data.update(kwargs)
                bundle = self.full_hydrate(bundle)
                lookup_kwargs = kwargs.copy()

                for key in kwargs.keys():
                    if key == 'pk':
                        continue
                    elif getattr(bundle.obj, key, NOT_AVAILABLE) is not NOT_AVAILABLE:
                        lookup_kwargs[key] = getattr(bundle.obj, key)
                    else:
                        del lookup_kwargs[key]
            except:
                # if there is trouble hydrating the data, fall back to just
                # using kwargs by itself (usually it only contains a "pk" key
                # and this will work fine.
                lookup_kwargs = kwargs

            try:
                bundle.obj = self.obj_get(bundle.request, **lookup_kwargs)
            except ObjectDoesNotExist:
                raise NotFound("A model instance matching the provided arguments could not be found.")

        bundle = self.full_hydrate(bundle)
        self.is_valid(bundle,request)

        if bundle.errors and not skip_errors:
            self.error_response(bundle.errors, request)

        # Save FKs just in case.
        self.save_related(bundle)

        # Save the main object.
        bundle.obj.save()

        # Now pick up the M2M bits.
        m2m_bundle = self.hydrate_m2m(bundle)
        self.save_m2m(m2m_bundle)
        return bundle

    def obj_delete_list(self, request=None, **kwargs):
        """
        A DRM-specific implementation of ``obj_delete_list``.

        Takes optional ``kwargs``, which can be used to narrow the query.
        """
        base_object_list = self.get_object_list(request).filter(**kwargs)
        authed_object_list = self.apply_authorization_limits(request, base_object_list)

        if hasattr(authed_object_list, 'delete'):
            # It's likely a ``QuerySet``. Call ``.delete()`` for efficiency.
            authed_object_list.delete()
        else:
            for authed_obj in authed_object_list:
                authed_obj.delete()

    def obj_delete(self, request=None, **kwargs):
        """
        A DRM-specific implementation of ``obj_delete``.

        Takes optional ``kwargs``, which are used to narrow the query to find
        the instance.
        """
        obj = kwargs.pop('_obj', None)

        if not hasattr(obj, 'delete'):
            try:
                obj = self.obj_get(request, **kwargs)
            except ObjectDoesNotExist:
                raise NotFound("A model instance matching the provided arguments could not be found.")

        obj.delete()

    def put_list_FIXME():
        # FIXME make according to comments, see discussion we had about this
        """ 
        - fetches the existing collection at the request URI with get_list

        NOTES: 
          * the URI may be that of a 'filtered collection', 
            e.g. /books?author=adams, or /books?id__in[]=1&id__in[]=3
          * nested collections are translated into a filtered version of their
            root resource URI, at least adding their relation to the 
            objects in the request URI.

        - determines which
        - updates the union of collection in this URI
        - determines the union with the new collection
        """
        pass
