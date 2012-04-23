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
    def __str__(self):
        return 'No default provided.'


DATE_REGEX = re.compile('^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}).*?$')
DATETIME_REGEX = re.compile('^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})(T|\s+)(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2}).*?$')


# All the ApiField variants.

class ApiField(object):
    """The base implementation of a field used by the resources."""
    dehydrated_type = 'string'
    help_text = ''

    def __init__(self, attribute=None, default=NOT_PROVIDED, null=False, blank=False, readonly=False, unique=False, help_text=None):
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

        Optionally accepts a ``null``, which indicated whether or not a
        ``None`` is allowable data on the field. Defaults to ``False``.

        Optionally accepts a ``blank``, which indicated whether or not
        data may be omitted on the field. Defaults to ``False``.

        Optionally accepts a ``readonly``, which indicates whether the field
        is used during the ``hydrate`` or not. Defaults to ``False``.

        Optionally accepts a ``unique``, which indicates if the field is a
        unique identifier for the object.

        Optionally accepts ``help_text``, which lets you provide a
        human-readable description of the field exposed at the schema level.
        Defaults to the per-Field definition.
        """
        # Track what the index thinks this field is called.
        self.instance_name = None
        self._resource = None
        self.attribute = attribute
        self._default = default
        self.null = null
        self.blank = blank
        self.readonly = readonly
        self.value = None
        self.unique = unique

        if help_text:
            self.help_text = help_text

    def contribute_to_class(self, cls, name):
        # Do the least we can here so that we don't hate ourselves in the
        # morning.
        self.instance_name = name
        self._resource = cls

    def has_default(self):
        """Returns a boolean of whether this field has a default value."""
        return self._default is not NOT_PROVIDED

    @property
    def default(self):
        """Returns the default value for the field."""
        if callable(self._default):
            return self._default()

        return self._default

    def get_field_attribute( self, bundle ):
        '''
        Returns the corresponding instance attribute
        ``attribute`` specifies which field on the model/document should
        be accessed to get data for this corresponding ApiField.
        ``attribute`` can contain Django-style (=MongoEngine style) 
        double underscores (`__`) to specify relations (of relations).
        
        For instance a 'store' can contain 'books' that have an 'author'.
        If we were to expose the names of authors whose books the store
        carries, we could say: 
          
          StringField( attribute='books__author__name' )
        '''
        if self.attribute is None:
            # This attribute should be manually accessed
            return None

        if callable( self.attribute ):
            return self.attribute()

        if isinstance( self.attribute, basestring ):
            attr_chain = self.attribute.split( '__' )

            previous_object = bundle.obj
            for attr in attr_chain:
                try:
                    current_object = getattr(previous_object, attr, None)
                except ObjectDoesNotExist:
                    current_object = None

                if current_object is None:
                    # We should fall out of the loop here since we cannot 
                    # access any more attributes down the chain.
                    if self.has_default():
                        current_object = self._default
                        break
                    elif self.null:
                        current_object = None
                        break
                    else:
                        raise ApiFieldError("The object '%r' has an empty attribute '%s' and doesn't allow a default or null value." % (previous_object, attr))

            if callable(current_object):
                return current_object()
            else:
                return current_object

    def dehydrate(self, bundle):
        """
        Takes data from the object and prepares it for the corresponding field
        on the resource.
        """
        obj = self.get_field_attribute( bundle )
        if obj:
            return self.convert(obj)
        elif self.has_default():
            if callable( self._default ):
                return self._default()

            return self.convert(self._default)
        else:
            return None

    def convert(self, value):
        """
        Handles conversion between the data found and the type of the field.

        Extending classes should override this method and provide correct
        data coercion.
        """
        return value

    def hydrate(self, bundle):
        """
        Takes data stored in the bundle for the field and returns it. Used for
        taking simple data and building an instance object.
        """
        if self.readonly:
            return None
        if not bundle.data.has_key(self.instance_name):
            if self.blank:
                return None
            elif self.attribute and getattr(bundle.obj, self.attribute, None):
                return getattr(bundle.obj, self.attribute)
            elif self.instance_name and hasattr(bundle.obj, self.instance_name):
                return getattr(bundle.obj, self.instance_name)
            elif self.has_default():
                if callable(self._default):
                    return self._default()

                return self._default
            elif self.null:
                return None
            else:
                raise ApiFieldError("The '%s' field has no data and doesn't allow a default or null value." % self.instance_name)

        return bundle.data[self.instance_name]


class ObjectIdField( ApiField ):
    """
    Field for representing the ObjectId from MongoDB.
    """
    
    help_text = "ObjectId field that corresponds to MongoDB's ObjectId"
    
    def __init__(self, attribute=None, default=NOT_PROVIDED, null=False, blank=False, readonly=False, unique=False, help_text=None):
        super(ObjectIdField, self).__init__(
                readonly=True, unique=True, blank=False, null=False, help_text=help_text)
        

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


class IntegerField(ApiField):
    """
    An integer field.

    Covers ``models.IntegerField``, ``models.PositiveIntegerField``,
    ``models.PositiveSmallIntegerField`` and ``models.SmallIntegerField``.
    """
    dehydrated_type = 'integer'
    help_text = 'Integer data. Ex: 2673'

    def convert(self, value):
        if value is None:
            return None

        return int(value)


class FloatField(ApiField):
    """
    A floating point field.
    """
    dehydrated_type = 'float'
    help_text = 'Floating point numeric data. Ex: 26.73'

    def convert(self, value):
        if value is None:
            return None

        return float(value)


class DecimalField(ApiField):
    """
    A decimal field.
    """
    dehydrated_type = 'decimal'
    help_text = 'Fixed precision numeric data. Ex: 26.73'

    def convert(self, value):
        if value is None:
            return None

        return Decimal(value)


class BooleanField(ApiField):
    """
    A boolean field.

    Covers both ``models.BooleanField`` and ``models.NullBooleanField``.
    """
    dehydrated_type = 'boolean'
    help_text = 'Boolean data. Ex: True'

    def convert(self, value):
        if value is None:
            return None

        return bool(value)


class ListField(ApiField):
    """
    A list field.
    """
    dehydrated_type = 'list'
    help_text = "A list of data. Ex: ['abc', 26.73, 8]"

    def convert(self, value):
        if value is None:
            return None

        return list(value)


class DictField(ApiField):
    """
    A dictionary field.
    """
    dehydrated_type = 'dict'
    help_text = "A dictionary of data. Ex: {'price': 26.73, 'name': 'Daniel'}"

    def convert(self, value):
        if value is None:
            return None

        return dict(value)


class DateField(ApiField):
    """
    A date field.
    """
    dehydrated_type = 'date'
    help_text = 'A date as a string. Ex: "2010-11-10"'

    def convert(self, value):
        if value is None:
            return None

        if isinstance(value, basestring):
            match = DATE_REGEX.search(value)

            if match:
                data = match.groupdict()
                return datetime.date(int(data['year']), int(data['month']), int(data['day']))
            else:
                raise ApiFieldError("Date provided to '%s' field doesn't appear to be a valid date string: '%s'" % (self.instance_name, value))

        return value

    def hydrate(self, bundle):
        value = super(DateField, self).hydrate(bundle)

        if value and not hasattr(value, 'year'):
            try:
                # Try to rip a date/datetime out of it.
                value = make_aware(parse(value))

                if hasattr(value, 'hour'):
                    value = value.date()
            except ValueError:
                pass

        return value


class DateTimeField(ApiField):
    """
    A datetime field.
    """
    dehydrated_type = 'datetime'
    help_text = 'A date & time as a string. Ex: "2010-11-10T03:07:43"'

    def convert(self, value):
        if value is None:
            return None

        if isinstance(value, basestring):
            match = DATETIME_REGEX.search(value)

            if match:
                data = match.groupdict()
                return make_aware(datetime.datetime(int(data['year']), int(data['month']), int(data['day']), int(data['hour']), int(data['minute']), int(data['second'])))
            else:
                raise ApiFieldError("Datetime provided to '%s' field doesn't appear to be a valid datetime string: '%s'" % (self.instance_name, value))

        return value

    def hydrate(self, bundle):
        value = super(DateTimeField, self).hydrate(bundle)

        if value and not hasattr(value, 'year'):
            try:
                # Try to rip a date/datetime out of it.
                value = make_aware(parse(value))
            except ValueError:
                pass

        return value


class TimeField(ApiField):
    dehydrated_type = 'time'
    help_text = 'A time as string. Ex: "20:05:23"'

    def dehydrate(self, obj):
        return self.convert(super(TimeField, self).dehydrate(obj))

    def convert(self, value):
        if isinstance(value, basestring):
            return self.to_time(value)
        return value

    def to_time(self, s):
        try:
            dt = parse(s)
        except ValueError, e:
            raise ApiFieldError(str(e))
        else:
            return datetime.time(dt.hour, dt.minute, dt.second)

    def hydrate(self, bundle):
        value = super(TimeField, self).hydrate(bundle)

        if value and not isinstance(value, datetime.time):
            value = self.to_time(value)

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

    def __init__(self, to, attribute, default=NOT_PROVIDED, null=False, blank=False, readonly=False, full=False, unique=False, help_text=None):
        """
        Builds the field and prepares it to access the related data.

        The ``to`` argument should point to a ``Resource`` class, NOT
        to a ``Model``. Required.

        The ``attribute`` argument should specify what field/callable points to
        the related data on the instance object. Required.

        Optionally accepts a ``null``, which indicated whether or not a
        ``None`` is allowable data on the field. Defaults to ``False``.

        Optionally accepts a ``blank``, which indicated whether or not
        data may be omitted on the field. Defaults to ``False``.

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
        self.instance_name = None
        self._resource = None
        self.to = to
        self.attribute = attribute
        self._default = default
        self.null = null
        self.blank = blank
        self.readonly = readonly
        self.full = full
        self.api = None
        self.resource_name = None
        self.unique = unique
        self._to_class = None

        if self.to == 'self':
            self.self_referential = True
            self._to_class = self.__class__

        if help_text:
            self.help_text = help_text

    def contribute_to_class(self, cls, name):
        super(RelatedField, self).contribute_to_class(cls, name)

        # Check if we're self-referential and hook it up.
        if self.self_referential or self.to == 'self':
            self._to_class = cls

    def get_related_resource(self, related_instance):
        """
        Instantiates the related resource.
        """
        related_resource = self.to_class()

        # Fix the ``api`` if it's not present.
        if related_resource._meta.api is None:
            if self._resource and not self._resource._meta.api is None:
                related_resource._meta.api = self._resource._meta.api

        # Try to be efficient about DB queries.
        related_resource.instance = related_instance
        return related_resource

    @property
    def to_class(self):
        # We need to be lazy here, because when the metaclass constructs the
        # Resources, other classes may not exist yet.
        # That said, memoize this so we never have to relookup/reimport.
        if self._to_class:
            return self._to_class

        if not isinstance(self.to, basestring):
            self._to_class = self.to
            return self._to_class

        # It's a string. Let's figure it out.
        if '.' in self.to:
            # Try to import.
            module_bits = self.to.split('.')
            module_path, class_name = '.'.join(module_bits[:-1]), module_bits[-1]
            module = importlib.import_module(module_path)
        else:
            # We've got a bare class name here, which won't work (No AppCache
            # to rely on). Try to throw a useful error.
            raise ImportError("TastyMongo requires a Python-style path (<module.module.Class>) to lazy load related resources. Only given '%s'." % self.to)

        self._to_class = getattr(module, class_name, None)

        if self._to_class is None:
            raise ImportError("Module '%s' does not appear to have a class called '%s'." % (module_path, class_name))

        return self._to_class

    def dehydrate_related(self, bundle, related_resource):
        """
        Returns either the endpoint or the data from ``full_dehydrate`` for the related resource.
        """
        if not self.full:
            # Return only the URI of the related resource
            return related_resource.get_resource_uri(bundle.request, bundle)
        else:
            # Return a fully dehydrated related resource
            bundle = related_resource.build_bundle(obj=related_resource.instance, request=bundle.request)
            return related_resource.full_dehydrate(bundle)

    def resource_from_uri(self, related_resource, uri, request=None ):
        """
        The related resource is attempted to be loaded based on the identifiers in the URI.
        """
        try:
            obj = related_resource.get_via_uri(uri, request=request)
            bundle = related_resource.build_bundle(obj=obj, request=request)
            return related_resource.full_dehydrate(bundle)
        except ObjectDoesNotExist:
            raise ApiFieldError("Could not find the provided object via resource URI '%s'." % uri)

    def resource_from_data(self, related_resource, data, request=None ):
        """
        Given a dictionary-like structure is provided, a fresh related
        resource is created using that data.
        """
        related_bundle = related_resource.build_bundle(data=data, request=request)

        # We need to check to see if updates are allowed on the related
        # resource. If not, we'll just return a populated bundle instead
        # of mistakenly updating something that should be read-only.
        if not related_resource.can_update():
            return related_resource.full_hydrate(related_bundle)

        try:
            return related_resource.obj_update(related_bundle, skip_errors=True, **data)
        except NotFound:
            try:
                # Attempt lookup by primary key
                lookup_kwargs = dict((k, v) for k, v in data.iteritems() if getattr(related_resource, k).unique)

                if not lookup_kwargs:
                    raise NotFound()
                return related_resource.obj_update(related_bundle, skip_errors=True, **lookup_kwargs)
            except NotFound:
                related_bundle = related_resource.full_hydrate(related_bundle)
                related_resource.is_valid(related_bundle, request)
                return related_bundle
        except MultipleObjectsReturned:
            return related_resource.full_hydrate(related_bundle)

    def build_related_resource(self, value, request=None ):
        """
        Returns a bundle of data built by the related resource, usually via
        ``hydrate`` with the data provided.

        Accepts either a URI or a data dictionary (or dictionary-like structure)
        """
        self.related_resource = self.to_class()
        kwargs = {
            'request': request,
        }

        if isinstance(value, basestring):
            # We got a URI. Load the object and assign it.
            return self.resource_from_uri(self.related_resource, value, **kwargs)
        elif hasattr(value, 'items'):
            # We've got a data dictionary. Construct the new object.
            return self.resource_from_data(self.related_resource, value, **kwargs)
        else:
            raise ApiFieldError("The '%s' field was given data that was not a URI and not a dictionary-alike: %s." % (self.instance_name, value))


class ToOneField( RelatedField ):
    """
    Provides access to singular related data.
    """
    help_text = 'A single related resource. Can be either a URI or nested resource data.'

    def dehydrate(self, bundle):

        related_obj = self.get_field_attribute( bundle )

        if not related_obj:
            return None

        self.related_resource = self.get_related_resource(related_obj)
        related_bundle = Bundle(obj=related_obj, request=bundle.request)
        return self.dehydrate_related(related_bundle, self.related_resource)

    def hydrate(self, bundle):
        value = super(ToOneField, self).hydrate(bundle)

        if value is None:
            return value

        return self.build_related_resource(value, request=bundle.request)


class ToManyField( RelatedField ):
    """
    Provides access to a list of related resources.
    """
    help_text = 'Many related resources. Can be either a list of URIs or a list of individually nested resource data.'

    def dehydrate(self, bundle):
        if not bundle.obj or not bundle.obj.pk:
            if self.null:
                return []

            raise ApiFieldError("The model '%r' does not have a primary key and can not be used in a ToMany context." % bundle.obj)

        related_objs = self.get_field_attribute( bundle )

        if not related_objs:
            return []

        dehydrated_bundles = []

        for related_obj in related_objs:
            related_resource = self.get_related_resource( related_obj )
            related_bundle = Bundle( obj=related_obj, request=bundle.request )
            dehydrated_bundles.append(self.dehydrate_related( related_bundle, related_resource ))

        return dehydrated_bundles 

