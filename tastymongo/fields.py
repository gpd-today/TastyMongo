from __future__ import print_function
from __future__ import unicode_literals

import datetime
import importlib
from dateutil.parser import parse
from decimal import Decimal
import re

from .exceptions import ApiFieldError
from .utils import *
from .bundle import Bundle


class NOT_PROVIDED:
    def __str__( self ):
        return 'No default provided.'


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
        self.value = None
        self.unique = unique

        if help_text:
            self.help_text = help_text

    def contribute_to_class( self, cls, name ):
        # Do the least we can here so that we don't hate ourselves in the morning.
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

    def to_data( self, value ):
        """
        Handles conversion from the object data to the type of the field.

        Extending classes should override this method and provide correct
        data coercion.
        """
        return value

    def hydrate( self, bundle ):
        """
        Takes data stored in the bundle for the field and returns it.

        If there's no data in the bundle for this field, return a default if
        given, None if allowed, or raise an ApiFieldError.
        """
        if self.readonly:
            return None

        # This is the default and what should happen most: the bundle has data.
        if self.field_name in bundle.data:
            return bundle.data[self.field_name]

        elif self.has_default:
            return self.default

        elif not self.required:
            return None
        else:
            raise ApiFieldError( "The '%s' field is required but has no data and doesn't have a default value." % self.field_name )

    def dehydrate( self, bundle ):
        '''
        Returns the Document's data for the field.

        ``attribute`` specifies which field on the object should
        be accessed to get data for this corresponding ApiField.

        ``attribute`` can contain MongoEngine style double underscores ( `__` ) 
        to specify relations ( of relations ).
        

        Example:
        -------

        A 'store' can contain 'books' that have an 'author'.
        If the resource were to expose the names of authors whose books 
        the store carries, we could say: 
          
        StoreResource( DocumentResource ):
            authornames = ListField( attribute='books__author__name' )

        '''
        if isinstance( self.attribute, basestring ):
            # `attribute` points to an attribute or method on the object.
            # Check for `__` in the field for looking through any relation.
            attr_chain = self.attribute.split( '__' )

            previous_object = bundle.obj
            for attr in attr_chain:
                try:
                    current_object = getattr( previous_object, attr, None )
                except ObjectDoesNotExist:
                    current_object = None

                if current_object is None:
                    # We should fall out of the loop here since we cannot 
                    # access any attributes further down the chain.
                    if self.has_default:
                        current_object = self.default
                        break
                    elif not self.required:
                        current_object = None
                        break
                    else:
                        raise ApiFieldError( "The object '%r' is required but has an empty attribute '%s' and doesn't have a default value ." % ( previous_object, attr ))

            if callable( current_object ):
                current_object = current_object()

            return self.to_data( current_object )

        elif callable( self.attribute ):
            # `attribute` is a method on the Resource that provides data.
            return self.attribute()

        elif self.has_default:
            return self.to_data(self.default)

        else:
            return None


class ObjectIdField( ApiField ):
    """
    Field for representing the ObjectId from MongoDB.
    """
    
    help_text = "ObjectId field that corresponds to MongoDB's ObjectId"
    
    def __init__( self, attribute=None, default=NOT_PROVIDED, required=False, readonly=False, unique=False, help_text=None ):
        super( ObjectIdField, self ).__init__(
                readonly=True, unique=True, required=False, help_text=help_text )
        

class StringField( ApiField ):
    """
    A text field of arbitrary length.
    """
    dehydrated_type = 'string'
    help_text = 'Unicode string data. Ex: "Hello World"'

    def to_data( self, value ):
        if value is None:
            return None

        return unicode( value )


class IntegerField( ApiField ):
    """
    An integer field.
    """
    dehydrated_type = 'integer'
    help_text = 'Integer data. Ex: 2673'

    def to_data( self, value ):
        if value is None:
            return None

        return int( value )


class FloatField( ApiField ):
    """
    A floating point field.
    """
    dehydrated_type = 'float'
    help_text = 'Floating point numeric data. Ex: 26.73'

    def to_data( self, value ):
        if value is None:
            return None

        return float( value )


class DecimalField( ApiField ):
    """
    A decimal field.
    """
    dehydrated_type = 'decimal'
    help_text = 'Fixed precision numeric data. Ex: 26.73'

    def to_data( self, value ):
        if value is None:
            return None

        return Decimal( value )


class BooleanField( ApiField ):
    """
    A boolean field.
    """
    dehydrated_type = 'boolean'
    help_text = 'Boolean data. Ex: True'

    def to_data( self, value ):
        if value is None:
            return None

        return bool( value )


class ListField( ApiField ):
    """
    A list field.
    """
    dehydrated_type = 'list'
    help_text = "A list of data. Ex: ['abc', 26.73, 8]"

    def to_data( self, value ):
        if value is None:
            return None

        return list( value )


class DictField( ApiField ):
    """
    A dictionary field.
    """
    dehydrated_type = 'dict'
    help_text = "A dictionary of data. Ex: {'price': 26.73, 'name': 'Daniel'}"

    def to_data( self, value ):
        if value is None:
            return None

        return dict( value )


class DateField( ApiField ):
    """
    A date field.
    """
    dehydrated_type = 'date'
    help_text = 'A date as a string. Ex: "2010-11-10"'

    def to_data( self, value ):
        if value is None:
            return None

        if isinstance( value, basestring ):
            match = DATE_REGEX.search( value )

            if match:
                data = match.groupdict()
                return datetime.date( int( data['year'] ), int( data['month'] ), int( data['day'] ))
            else:
                raise ApiFieldError( "Date provided to '%s' field doesn't appear to be a valid date string: '%s'" % ( self.field_name, value ))

        return value

    def hydrate( self, bundle ):
        value = super( DateField, self ).hydrate( bundle )

        if value and not hasattr( value, 'year' ):
            try:
                # Try to rip a date/datetime out of it.
                value = make_aware( parse( value ))

                if hasattr( value, 'hour' ):
                    value = value.date()
            except ValueError:
                pass

        return value


class DateTimeField( ApiField ):
    """
    A datetime field.
    """
    dehydrated_type = 'datetime'
    help_text = 'A date & time as a string. Ex: "2010-11-10T03:07:43"'

    def to_data( self, value ):
        if value is None:
            return None

        if isinstance( value, basestring ):
            match = DATETIME_REGEX.search( value )

            if match:
                data = match.groupdict()
                return make_aware( datetime.datetime( int( data['year'] ), int( data['month'] ), int( data['day'] ), int( data['hour'] ), int( data['minute'] ), int( data['second'] )) )
            else:
                raise ApiFieldError( "Datetime provided to '%s' field doesn't appear to be a valid datetime string: '%s'" % ( self.field_name, value ))

        return value

    def hydrate( self, bundle ):
        value = super( DateTimeField, self ).hydrate( bundle )

        if value and not hasattr( value, 'year' ):
            try:
                # Try to rip a date/datetime out of it.
                value = make_aware( parse( value ))
            except ValueError:
                pass

        return value


class TimeField( ApiField ):
    dehydrated_type = 'time'
    help_text = 'A time as string. Ex: "20:05:23"'

    def to_data( self, value ):
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
        value = super( TimeField, self ).hydrate( bundle )

        if value and not isinstance( value, datetime.time ):
            value = self.to_time( value )

        return value


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

    def __init__( self, to, attribute, default=NOT_PROVIDED, required=False, readonly=False, full=False, unique=False, help_text=None ):
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
        self.to = to
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
                raise ImportError( "TastyMongo could not resolve the path `%s` for resource `%s`" % ( self.to, class_name ) )
        else:
            # We've got a bare class name here, which won't work ( No AppCache
            # to rely on ). Try to throw a useful error.
            raise ImportError( "TastyMongo requires a Python-style path ( <module.module.Class> ) to lazy load related resources. Only given '%s'." % self.to )

        self._to_class = getattr( module, class_name, None )

        if self._to_class is None:
            raise ImportError( "Module '%s' does not appear to have a class called '%s'." % ( module_path, class_name ))

        return self._to_class

    def get_related_resource( self ):
        """
        Instantiates the related resource.
        """
        related_resource = self.to_class()

        # Fix the ``api`` if it's not present.
        if related_resource._meta.api is None:
            if self._resource and not self._resource._meta.api is None:
                related_resource._meta.api = self._resource._meta.api

        return related_resource

    def build_related_bundle(self, value, request=None ):
        """
        Returns a bundle built by the related resource. The related Resource's 
        hydrate method is used to populate the related object from related data.
        This may cause recursion for deeper nested data.

        Accepts either a URI or a dictionary-like structure.
        """
        related_resource = self.get_related_resource()

        if  isinstance(value, Bundle):
            # We got a bundle, just return it. The bundle may have been 
            # generated by a method on the resource or object.
            return value
        elif isinstance(value, basestring):
            # We got a resource URI. Try to create a bundle with the resource.
            return related_resource.bundle_from_uri( value, request=request )
        elif hasattr(value, 'items'):
            # We've got a data dictionary. 
            return related_resource.bundle_from_data( value, request=request )
        else:
            raise ApiFieldError("The '%s' field was given data that was not a URI and not a dictionary-alike: %s." % (self.instance_name, value))

    def hydrate( self, bundle ):
        '''
        Hydrate for related fields is a little more involved.
        
        When there's data for the field, don't just return it, but create 
        a related bundle with the data and the related object in it. 
       
        It calls upon the related resource's hydrate method to instantiate the 
        object. The related resource may in turn recurse for nested data.
        '''
        value = super( RelatedField, self ).hydrate( bundle )

        if value is None:
            return None

        return self.build_related_bundle( value, request=bundle.request )

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

        related_resource = self.get_related_resource()
        related_bundle = related_resource.build_bundle( obj=related_object, request=bundle.request )

        if not self.full:
            return related_resource.get_resource_uri( bundle.request, related_bundle )
        else:
            return related_resource.dehydrate( related_bundle )


class ToOneField( RelatedField ):
    """
    Provides access to singular related data.
    """
    help_text = 'A single related resource. Can be either a URI or nested resource data.'


class ToManyField( RelatedField ):
    """
    Provides access to a list of related resources.
    """
    help_text = 'Many related resources. Can be either a list of URIs or a list of individually nested resource data.'
    is_tomany = True

    def hydrate( self, bundle ):
        '''
        ToManyFields may have many related bundles, so `hydrate` should return
        a list instead of a single bundle.
        '''
        values_list = super( RelatedField, self ).hydrate( bundle )

        if not values_list:
            # Could be None or an empty list:
            return []

        try:
            return [self.build_related_bundle( value, request=bundle.request ) for value in values_list]
        except TypeError:
            # values_list is not iterable, assume a single item and return it in a list
            return [self.build_related_bundle( values_list, request=bundle.request )]

    def dehydrate( self, bundle ):
        """
        Returns the URIs only or the (nested) data for the related resources.

        When fully populated objects are requested, pick up the objects
        for the related resource, create a bundle for them and call upon 
        the related resource's dehydrate method to populate the data from
        the object. The related resources may in turn recurse for nested data.
        """
        related_objects = super( RelatedField, self ).dehydrate( bundle )
        if related_objects is None:
            related_objects = []

        related_resource = self.get_related_resource()
        related_bundles = []
        for related_object in related_objects:
            related_bundle = related_resource.build_bundle( obj=related_object, request=bundle.request )
            if not self.full:
                related_bundles.append( related_resource.get_resource_uri( bundle.request, related_bundle ))
            else:
                related_bundles.append( related_resource.dehydrate( related_bundle ))

        return related_bundles

