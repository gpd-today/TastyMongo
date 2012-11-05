from __future__ import print_function
from __future__ import unicode_literals

def determine_format(request, serializer, default_format='application/json'):
    """
    Tries to "smartly" determine which output format is desired.
    
    First attempts to find a ``format`` override from the request and supplies
    that if found.
    
    If no request format was demanded, it falls back to ``mimeparse`` and the
    ``Accepts`` header, allowing specification that way.
    
    If still no format is found, returns the ``default_format`` (which defaults
    to ``application/json`` if not provided).
    """
    # First, check if they forced the format.
    if 'format' in request.GET and request.GET['format'] in serializer.formats:
        return serializer.get_mime_for_format(request.GET['format'])
    
    # If callback parameter is present, use JSONP.
    if 'callback' in request.GET:
        return serializer.get_mime_for_format('jsonp')
    
    # Try to fallback on the Accepts header.
    if request.accept != '*/*':
        formats = list(serializer.supported_formats) or []
        best_format = request.accept.best_match(formats)
        
        if best_format:
            return best_format
    
    # No valid 'Accept' header/formats. Sane default.
    return default_format


def build_content_type(format, encoding='utf-8'):
    """
    Appends character encoding to the provided format if not already present.
    """
    if 'charset' in format:
        return format
    
    return str("{0}; charset={1}".format(format, encoding))
