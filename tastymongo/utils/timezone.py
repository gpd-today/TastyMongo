from __future__ import print_function
from __future__ import unicode_literals

import pytz 

def to_naive_utc( dt ):
    # Convert any timezone-aware datetime to UTC
    # see: http://docs.python.org/library/datetime.html#datetime.tzinfo
    is_aware = dt.tzinfo is not None and dt.tzinfo.utcoffset( dt ) is not None

    if is_aware:
        # Force UTC
        utc = pytz.utc
        dt = utc.normalize( dt )
        dt = dt.replace(tzinfo=None)

    return dt
