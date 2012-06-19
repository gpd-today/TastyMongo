from __future__ import print_function
from __future__ import unicode_literals

import datetime
import json

from .exceptions import *
from .bundle import Bundle
from .utils import *


class Serializer(object):
    """
    A swappable class for serialization.

    This handles most types of data as well as the following output formats::

        * json
        * jsonp
        * xml
        * yaml
        * html
        * plist (see http://explorapp.com/biplist/)

    It was designed to make changing behavior easy, either by overridding the
    various format methods (i.e. ``to_json``), by changing the
    ``formats/content_types`` options or by altering the other hook methods.
    """
    formats = ['json', 'html' ]
    content_types = {
        'json': 'application/json',
        'html': 'text/html'
    }

    def __init__(self, formats=None, content_types=None, datetime_formatting=None):
        self.supported_formats = []
        self.datetime_formatting = 'iso-8601'

        if formats is not None:
            self.formats = formats

        if content_types is not None:
            self.content_types = content_types

        if datetime_formatting is not None:
            self.datetime_formatting = datetime_formatting

        for format in self.formats:
            try:
                self.supported_formats.append(self.content_types[format])
            except KeyError:
                raise ConfigurationError("Content type for specified type '{}' not found. Please provide it at either the class level or via the arguments.".format( format ) )

    def get_mime_for_format(self, format):
        """
        Given a format, attempts to determine the correct MIME type.

        If not available on the current ``Serializer``, returns
        ``application/json`` by default.
        """
        try:
            return self.content_types[format]
        except KeyError:
            return 'application/json'

    def format_datetime(self, data):
        """
        A hook to control how datetimes are formatted.

        Default is ``iso-8601``, which looks like "2010-12-16T03:02:14".
        """
        data = make_naive(data)

        return data.isoformat()

    def format_date(self, data):
        """
        A hook to control how dates are formatted.

        Default is ``iso-8601``, which looks like "2010-12-16".
        """
        return data.isoformat()

    def format_time(self, data):
        """
        A hook to control how times are formatted.

        Default is ``iso-8601``, which looks like "03:02:14".
        """

        return data.isoformat()

    def serialize(self, bundle, format='application/json', options={}):
        """
        Given some data and a format, calls the correct method to serialize
        the data and returns the result.
        """
        desired_format = None

        for short_format, long_format in self.content_types.items():
            if format == long_format:
                if hasattr(self, "to_%s" % short_format):
                    desired_format = short_format
                    break

        if desired_format is None:
            raise UnsupportedFormat("The format indicated '{}' had no available serialization method. Please check your ``formats`` and ``content_types`` on your Serializer.".format( format ) )

        serialized = getattr(self, "to_{}".format( desired_format ) )(bundle, options)
        return serialized

    def deserialize(self, content, format='application/json'):
        """
        Given some data and a format, calls the correct method to deserialize
        the data and returns the result.
        """
        desired_format = None

        format = format.split(';')[0]

        for short_format, long_format in self.content_types.items():
            if format == long_format:
                if hasattr(self, "from_%s" % short_format):
                    desired_format = short_format
                    break

        if desired_format is None:
            raise UnsupportedFormat("The format indicated '{}' had no available deserialization method. Please check your ``formats`` and ``content_types`` on your Serializer.".format( format ) )

        deserialized = getattr(self, "from_%s" % desired_format)(content)
        return deserialized

    def to_simple(self, data, options):
        """
        For a piece of data, attempts to recognize it and provide a simplified
        form of something complex.

        This brings complex Python data structures down to native types of the
        serialization format(s).
        """
        if isinstance(data, (list, tuple)):
            return [self.to_simple(item, options) for item in data]
        if isinstance(data, dict):
            return dict((key, self.to_simple(val, options)) for (key, val) in data.iteritems())
        elif isinstance(data, Bundle):
            return dict((key, self.to_simple(val, options)) for (key, val) in data.data.iteritems())
        elif isinstance(data, datetime.datetime):
            return self.format_datetime(data)
        elif isinstance(data, datetime.date):
            return self.format_date(data)
        elif isinstance(data, datetime.time):
            return self.format_time(data)
        elif isinstance(data, bool):
            return data
        elif type(data) in (long, int, float):
            return data
        elif data is None:
            return None
        else:
            return str(data)

    def to_json(self, data, options=None):
        """
        Given some Python data, produces JSON output.
        """
        options = options or {}
        data = self.to_simple(data, options)
        return json.dumps(data, sort_keys=True)

    def from_json(self, content):
        """
        Given some JSON data, returns a Python dictionary of the decoded data.
        """
        return json.loads(content)

    def to_jsonp(self, data, options=None):
        """
        Given some Python data, produces JSON output wrapped in the provided
        callback.
        """
        options = options or {}
        return '%s(%s)' % (options['callback'], self.to_json(data, options))

    def to_html(self, data, options=None):
        """
        Reserved for future usage.

        Provide HTML output of a resource, making an API available to a browser.
        """
        options = options or {}
        data = self.to_simple( data, options )
        js = json.dumps( data, sort_keys=True, indent=4 )
        html = '<html><body><pre>{}</pre></body></html>'.format( js );
        return html

    def from_html(self, content):
        """
        Reserved for future usage.

        The desire is to handle form-based (maybe Javascript?) input, making an
        API available to a browser. This is on the TODO list but not currently
        implemented.
        """
        pass

def get_type_string(data):
    """
    Translates a Python data type into a string format.
    """
    data_type = type(data)

    if data_type in (int, long):
        return 'integer'
    elif data_type == float:
        return 'float'
    elif data_type == bool:
        return 'boolean'
    elif data_type in (list, tuple):
        return 'list'
    elif data_type == dict:
        return 'hash'
    elif data is None:
        return 'null'
    elif isinstance(data, basestring):
        return 'string'
