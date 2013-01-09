from __future__ import print_function
from __future__ import unicode_literals

import datetime
import importlib
from dateutil.parser import parse
from decimal import Decimal
import re

from .exceptions import ApiFieldError
from .utils import *
from mongoengine import Document
from mongoengine.errors import ValidationError as MongoEngineValidationError

from .bundle import Bundle


class NOT_PROVIDED:
    def __str__( self ):
        return 'No default provided.'



# Define a `may_read` method, which uses the `PrivilegeMixin` to test if read is allowed if found.
# Alternatively, all documents returned by a resource may be read (since it has passed `authorization`).
try:
    from mongoengine_privileges.privilegemixin import PrivilegeMixin

    def may_read( doc, request ):
        # A document may be read if it doesn't implement privileges, or grants the request's user the `read` privilege
        return not isinstance( doc, PrivilegeMixin ) or doc.may( request, 'read' )

except ImportError:
    def may_read( doc, request ):
        return True


DATE_REGEX = re.compile( '^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}).*?$' )
DATETIME_REGEX = re.compile( '^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})( T|\s+)(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}).*?$' )


# All the ApiField variants.

class ApiField( object ):
    """The base implementation of a field used by the resources."""
    dehydrated_type = 'string'
    help_text = 'Generic field. Should not be used directly but subclassed.'

    def __init__( self, attribute=None, default=NOT_PROVIDED, required=False, readonly=False, unique=False, help_text=None ):
        """
        Sets up the field. This is generally called when the containing
        ``Resource`` is initialized.

        Optionally accepts an ``attribute``, which should be a string of
        either an instance attribute or callable of the object during the
        ``dehydrate`` or push data onto an object during the ``hydrate``.
        Defaults to ``None``, meaning data will be manually accessed.

        Optionally accepts a ``default``, which provides default data when the
        object being ``dehydrated``/``hydrated`` has no data on the field.
        Defaults to ``NOT_PROVIDED``.

        Optionally accepts a ``required``. Defaults to ``False``.

        Optionally accepts a ``readonly``, which indicates whether the field
        is used during the ``hydrate`` or not. Defaults to ``False``.

        Optionally accepts a ``unique``, which indicates if the field is a
        unique identifier for the object.

        Optionally accepts ``help_text``, which lets you provide a
        human-readable description of the field exposed at the schema level.
        Defaults to the per-Field definition.
        """
        # Track what the index thinks this field is called.
        self.field_name = None
        self._resource = None
        self.attribute = attribute
        self._default = default
        self.required = required
        self.readonly = readonly
        self.unique = unique

        if help_text:
            self.help_text = help_text

    def contribute_to_class( self, cls, name ):
        # FIXME: find out how to make this a little more transparent
        self.field_name = name
        self._resource = cls

    @property
    def has_default( self ):
        """Returns a boolean of whether this field has a default value.

           Split into a separate function to allow default values whose bool()
           would yield False
        """
        return self._default is not NOT_PROVIDED

    @property
    def default( self ):
        """Returns the default value for the field."""
        if callable( self._default ):
            return self._default()

        return self._default

    def convert( self, value ):
        """
        Handles conversion from the object data to the type of the field.

        Extending classes should override this method and provide correct
        data coercion.
        """
        return value

    def hydrate( self, bundle ):
        """
        Returns any data for the field that is present in the bundle.

        If there's no data for this field, return a default value if given,
        None if the field is not required, or raise ApiFieldError.
        """

        if self.field_name in bundle.data: 
            # The bundle has data for this field. Return it.
            data = self.convert(bundle.data[ self.field_name ])

        elif self.has_default:
            # The bundle has no data, but there's a default value for the field.
            data = self.default

        elif not self.required:
            # There's no default but the field is not required. 
            data = None

        else:
            # We're seriously out of options here.
            raise ApiFieldError( 'field `{0}` has no data in bundle `{1}` and no default.'.format( self.field_name, bundle ))

        return data

    def dehydrate( self, bundle ):
        '''
        Returns the Document's data for the field.

        ``attribute`` specifies which field on the object should
        be accessed to get data for this corresponding ApiField.
        '''
        if isinstance( self.attribute, basestring ):
            # `attribute` points to an attribute or method on the object.
            attr = bundle.obj[ self.attribute ]

            if isinstance( attr, Document ):
                if not may_read( attr, bundle.request ):
                    attr = None

            if isinstance( attr, list ):
                # Check permissions for each document
                attr = [ obj for obj in attr if may_read( obj, bundle.request ) ]

            if attr is None:
                if self.has_default:
                    attr = self.default
                elif not self.required:
                    attr = None
                else:
                    raise ApiFieldError( "Required attribute=`{}` on object=`{}` is empty, and does not have a default value.".format( attr, prev ) )

            return self.convert( attr )

        elif callable( self.attribute ):
            # `attribute` is a method on the Resource that provides data.
            return self.attribute()

        elif self.has_default:
            return self.convert( self.default )

        else:
            return None


class ObjectIdField( ApiField ):
    """
    Field for representing the ObjectId from MongoDB.
    """
    
    help_text = "ObjectId field that corresponds to MongoDB's ObjectId"
    
    def __init__( self, attribute=None, default=NOT_PROVIDED, required=False, readonly=True, unique=True, help_text='A MongoEngine ObjectId' ):
        super( ObjectIdField, self ).__init__( attribute='id',
                default=NOT_PROVIDED, required=required, readonly=True,
                unique=True, help_text='A MongoEngine ObjectId' )

    def dehydrate( self, bundle ):
        return bundle.obj.id
        

class StringField( ApiField ):
    """
    A text field of arbitrary length.
    """
    dehydrated_type = 'string'
    help_text = 'Unicode string data. Ex: "Hello World"'

    def convert( self, value ):
        if value is None:
            return None

        return unicode( value )


class IntegerField( ApiField ):
    """
    An integer field.
    """
    dehydrated_type = 'integer'
    help_text = 'Integer data. Ex: 2673'

    def convert( self, value ):
        if value is None:
            return None

        return int( value )


class FloatField( ApiField ):
    """
    A floating point field.
    """
    dehydrated_type = 'float'
    help_text = 'Floating point numeric data. Ex: 26.73'

    def convert( self, value ):
        if value is None:
            return None

        return float( value )


class DecimalField( ApiField ):
    """
    A decimal field.
    """
    dehydrated_type = 'decimal'
    help_text = 'Fixed precision numeric data. Ex: 26.73'

    def convert( self, value ):
        if value is None:
            return None

        return Decimal( value )


class BooleanField( ApiField ):
    """
    A boolean field.
    """
    dehydrated_type = 'boolean'
    help_text = 'Boolean data. Ex: True'

    def convert( self, value ):
        if value is None:
            return None

        return bool( value )


class ListField( ApiField ):
    """
    A list field.
    """
    dehydrated_type = 'list'
    help_text = "A list of data. Ex: ['abc', 26.73, 8]"

    def convert( self, value ):
        if value is None:
            return None

        return list( value )


class DictField( ApiField ):
    """
    A dictionary field.
    """
    dehydrated_type = 'dict'
    help_text = "A dictionary of data. Ex: {'type': 'dog', 'name': 'fido'}"

    def convert( self, value ):
        if value is None:
            return None

        return dict( value )


class EmbeddedDocumentField( ApiField ):
    """
    An Embedded Document Field. 
    """
    dehydrated_type = 'dict'
    help_text = "A dictionary with the underlying Document's field names as keys."

    def convert( self, value ):
        if not value:
            return None

        # Use the fields on the EmbeddedDocument to do type coercion.
        # Return a dict that only contains values actually in `value`: it will
        # be validated and transformed into/from an EmbeddedDocument by the
        # (de)hydrate methods.
        doc = self._resource._meta.object_class._fields[ self.field_name ].document_type()
        dct = {}
        for k, f in doc._fields.items():
            if k in value:
                api_field_class = self._resource.api_field_from_mongoengine_field( f )()
                dct[k] = api_field_class.convert( value[k] )

        return dct

    def hydrate( self, bundle ):
        # Get any existing EmbeddedDocument from the parent obj, update it with
        # the fields given in the bundle and validate it afterwards.
        dct = super( EmbeddedDocumentField, self).hydrate( bundle )

        if dct is None and not self.required:
            return None

        doc = getattr(bundle.obj, self.field_name) or self._resource._meta.object_class._fields[ self.field_name ].document_type()

        for k, v in dct.items():
            doc[k] = v

        try:
            doc.validate()
            return doc
        except MongoEngineValidationError:
            raise


class DateField( ApiField ):
    """
    A date field.
    """
    dehydrated_type = 'date'
    help_text = 'A date as a string. Ex: "2010-11-10"'

    def convert( self, value ):
        if not value:
            return None

        if isinstance( value, basestring ):
            match = DATE_REGEX.search( value )

            if match:
                data = match.groupdict()
                return datetime.date( int( data['year'] ), int( data['month'] ), int( data['day'] ))
            else:
                raise ApiFieldError( "Date `{0}` provided to the `{1}` field doesn't appear to be a valid date string: ".format( value, self.field_name) )

        if isinstance( value, datetime.datetime ):
            return datetime.date( value.year, value.month, value.day )

        return value

    def hydrate( self, bundle ):
        data = super( DateField, self ).hydrate( bundle )

        if data and not hasattr( data, 'year' ):
            try:
                # Try to rip a date/datetime out of it.
                data = make_aware( parse( data ))

                if hasattr( data, 'hour' ):
                    data = data.date()
            except ValueError:
                pass

        return data


class DateTimeField( ApiField ):
    """
    A datetime field.
    """
    dehydrated_type = 'datetime'
    help_text = 'A date & time as a string. Ex: "2010-11-10T03:07:43"'

    def convert( self, value ):
        if not value:
            return None

        if isinstance( value, basestring ):
            match = DATETIME_REGEX.search( value )
            
            if match:
                data = match.groupdict()
                return make_aware( datetime.datetime( int( data['year'] ), int( data['month'] ), int( data['day'] ), int( data['hour'] ), int( data['minute'] ), int( data['second'] )) )

            # MongoDB doesn't have separate `dates`, only `datetimes`, so if
            # `value` is a date string without datetime, pad it with 0 time.
            match = DATE_REGEX.search( value )
            if match: 
                data = match.groupdict()
                return make_aware( datetime.datetime( int( data['year'] ), int( data['month'] ), int( data['day'] ), 0, 0, 0 ))

            else:
                raise ApiFieldError( "Datetime `{0}` provided to `{1}` field doesn't appear to be a valid datetime string.".format( value, self.field_name ))

        return value

    def hydrate( self, bundle ):
        data = super( DateTimeField, self ).hydrate( bundle )

        if data and not hasattr( data, 'year' ):
            try:
                # Try to rip a date/datetime out of it.
                data = make_aware( parse( data ))
            except ValueError:
                pass

        return data


class TimeField( ApiField ):
    dehydrated_type = 'time'
    help_text = 'A time as string. Ex: "20:05:23"'

    def convert( self, value ):
        if not value:
            return None

        if isinstance( value, basestring ):
            return self.to_time( value )

        return value

    def to_time( self, s ):
        try:
            dt = parse( s )
        except ValueError, e:
            raise ApiFieldError( str( e ))
        else:
            return datetime.time( dt.hour, dt.minute, dt.second )

    def hydrate( self, bundle ):
        data = super( TimeField, self ).hydrate( bundle )

        if data and not isinstance( data, datetime.time ):
            data = self.to_time( data )

        return data


class RelatedField( ApiField ):
    """
    Provides access to data that is related within the database.

    The contents of this field actually point to another ``Resource``,
    rather than the related object. This allows the field to represent its data
    in different ways.
    """
    dehydrated_type = 'related'
    is_related = True
    self_referential = False
    help_text = 'A related resource. Can be either a URI or set of nested resource data.'

    def __init__( self, attribute, to=None, default=NOT_PROVIDED, required=False, readonly=False, full=False, unique=False, help_text=None ):
        """
        Builds the field and prepares it to access the related data.

        The ``to`` argument should point to a ``Resource`` class, NOT
        to a ``Document``. Required.

        The ``attribute`` argument should specify what field/callable points to
        the related data on the instance object. Required.

        Optionally accepts a ``required``. Defaults to ``False``.

        Optionally accepts a ``readonly``, which indicates whether the field
        is used during the ``hydrate`` or not. Defaults to ``False``.

        Optionally accepts a ``full``, which indicates how the related
        ``Resource`` will appear post-``dehydrate``. If ``False``, the
        related ``Resource`` will appear as a URL to the endpoint of that
        resource. If ``True``, the result of the sub-resource's
        ``dehydrate`` will be included in full.

        Optionally accepts a ``unique``, which indicates if the field is a
        unique identifier for the object.

        Optionally accepts ``help_text``, which lets you provide a
        human-readable description of the field exposed at the schema level.
        Defaults to the per-Field definition.
        """
        super( RelatedField, self ).__init__( attribute=attribute, default=default, required=required, readonly=readonly, unique=unique, help_text=help_text ) 

        # Set some properties specific to RelatedFields
        if to:
            self.to = to
        else:
            self.to = None

        self._to_class = None
        self.full = full

        if self.to == 'self':
            self.self_referential = True
            self._to_class = self.__class__

        if help_text:
            self.help_text = help_text

    def contribute_to_class( self, cls, name ):
        super( RelatedField, self ).contribute_to_class( cls, name )

        # Check if we're self-referential and hook it up.
        if self.self_referential or self.to == 'self':
            self._to_class = cls

    @property
    def to_class( self ):
        # We need to be lazy here, because when the metaclass constructs the
        # Resources, other classes may not exist yet.
        # That said, memoize this so we never have to relookup/reimport.
        if self._to_class:
            return self._to_class

        if not isinstance( self.to, basestring ):
            self._to_class = self.to
            return self._to_class

        # It's a string. Let's figure it out.
        if '.' in self.to:
            # Try to import.
            module_bits = self.to.split( '.' )
            module_path, class_name = '.'.join( module_bits[:-1] ), module_bits[-1]
            try:
                module = importlib.import_module( module_path )
            except ImportError:
                raise ImportError( "TastyMongo could not resolve the path `{0}` for resource `{1}`.".format( self.to, class_name ) )
        else:
            # We've got a bare class name here, which won't work ( No AppCache
            # to rely on ). Try to throw a useful error.
            raise ImportError( "TastyMongo requires a Python-style path ( <module.module.Class> ) to lazy load related resources. Only given `{0}`.".format( self.to ) )

        self._to_class = getattr( module, class_name, None )

        if self._to_class is None:
            raise ImportError( "Module `{0}` does not appear to have a class called `{1}`.".format( module_path, class_name ))

        return self._to_class

    def get_related_resource( self, data=None ):
        """
        Instantiates the related resource.

        @param data: if this field references a `GenericReferenceField`, `data` is used to determine what type
            of resource is applicable.
        @type data: Document or Bundle or dict
        """
        if self.to:
            related_resource = self.to_class()
        elif data:
            related_resource = None

            if isinstance( data, Document ):
                related_resource = self._resource._meta.api.resource_for_document( data )
            elif isinstance( data, Bundle ):
                if data.obj and isinstance( data.obj, Document ):
                    related_resource = self._resource._meta.api.resource_for_document( data.obj )
                else:
                    data = data.data

            if not related_resource:
                if isinstance( data, dict ) and self.field_name in data:
                    data = data[ self.field_name ]

                if 'resource_uri' in  data:
                    data = data[ 'resource_uri' ]

                related_resource = self._resource._meta.api.resource_from_uri( data )
        else:
            raise ValueError( 'Unable to resolve a related_resource for `{}.{}`'.format( self._resource._meta.resource_name, self.field_name ) )

        # Fix the ``api`` if it's not present.
        if related_resource._meta.api is None:
            if self._resource and not self._resource._meta.api is None:
                related_resource._meta.api = self._resource._meta.api

        return related_resource

    def get_related_bundle( self, data, request ):
        """
        Returns a bundle built and hydrated by the related resource. 
        Accepts either a URI or a dictionary-like structure.
        """
        related_resource = self.get_related_resource( data )

        if isinstance( data, basestring ):
            # We got a resource URI. Try to create a bundle with the resource.
            return related_resource.build_bundle( request=request, data=data )
        elif hasattr( data, 'items' ):
            # We've got a data dictionary. 
            if self.readonly:
                if 'resource_uri' in data:
                    # Ignore any other posted data and just return a URI.
                    return related_resource.build_bundle( request=request, data=data['resource_uri'] )
                else:
                    raise ApiFieldError("The `{0}` field was given data but is readonly: `{1}`.".format( self.field_name, data ) )
            else:
                related_bundle = related_resource.build_bundle( request=request, data=data )
                return related_resource.hydrate( related_bundle )
        else:
            raise ApiFieldError("The `{0}` field was given data that was not a URI and not a dictionary-alike: `{1}`.".format( self.field_name, data ) )


class ToOneField( RelatedField ):
    """
    Provides access to singular related data.
    """
    help_text = 'A single related resource. Can be either a URI or nested resource data.'

    def hydrate( self, bundle ):
        """
        When there's data for the field, create a related bundle with the data 
        and a new or existing related object in it. 
       
        It calls upon the related resource's hydrate method to instantiate the 
        object. The related resource may in turn recurse for nested data.
        """
        related_data = super( ToOneField, self ).hydrate( bundle )
        if related_data is None:
            return None

        return self.get_related_bundle( related_data, request=bundle.request )

    def dehydrate( self, bundle ):
        """
        Returns the URI only or the (nested) data for the related resource.

        When a fully populated object is requested, pick up the object 
        for the related resource, create a bundle for it and call upon 
        the related resource's dehydrate method to populate the data from
        the object. The related resource may in turn recurse for nested data.
        """
        related_object = super( RelatedField, self ).dehydrate( bundle )
        if related_object is None:
            # ApiField's `dehydrate` will have raised an ApiFieldError if 
            # the object may not be None, so we can safely return None.
            return None

        related_resource = self.get_related_resource( related_object )
        related_bundle = related_resource.build_bundle( request=bundle.request, obj=related_object )

        if not self.full:
            return related_resource.get_resource_uri( bundle.request, related_bundle )
        else:
            return related_resource.dehydrate( related_bundle )


class ToManyField( RelatedField ):
    """
    Provides access to a list of related resources.
    """
    help_text = 'Many related resources. Can be either a list of URIs or a list of individually nested resource data.'
    is_tomany = True

    def hydrate( self, bundle ):
        '''
        Returns the data from the Resource in a form ready to be set on documents. 

        When just a resource_uri is given, either as a string or within a dict,
        try to find the document, otherwise instantiate a new document.

        When other data is given as well, try to set it on the document's 
        corresponding attributes.
        
        Returns a list of bundles or an empty list.
        '''
        related_data = super( ToManyField, self ).hydrate( bundle )
        if related_data is None:
            return []

        return [self.get_related_bundle( related_item, request=bundle.request ) for related_item in related_data if related_item]

    def dehydrate( self, bundle ):
        """
        Returns the URIs only or the (nested) data for the related resources.

        When fully populated objects are requested, pick up the objects
        for the related resource, create a bundle for them and call upon 
        the related resource's dehydrate method to populate the data from
        the object. The related resources may in turn recurse for nested data.
        """
        related_objects = super( ToManyField, self ).dehydrate( bundle )
        if related_objects is None:
            related_objects = []

        related_resource = self.get_related_resource( bundle )
        related_bundles = [related_resource.build_bundle( request=bundle.request, obj=obj ) for obj in related_objects]
        if not self.full:
            related_bundles = [related_resource.get_resource_uri( bundle.request, b) for b in related_bundles]
        else:
            related_bundles = related_resource.dehydrate( related_bundles )

        return related_bundles

