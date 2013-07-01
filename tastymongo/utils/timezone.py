from __future__ import print_function
from __future__ import unicode_literals

import pytz 

def make_naive( dt ):
    # Convert any timezone-aware datetime to naive UTC
    # see: http://docs.python.org/library/datetime.html#datetime.tzinfo
    is_aware = dt.tzinfo is not None and dt.tzinfo.utcoffset( dt ) is not None

    if is_aware:
        utc = pytz.utc
        dt = utc.normalize( dt )
        dt = dt.replace( tzinfo=None )

    return dt

def make_aware( dt ):
    if dt.tzinfo is None:
        # assume UTC
        dt = dt.replace( tzinfo=pytz.UTC )

    return dt

