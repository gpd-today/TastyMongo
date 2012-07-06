from __future__ import print_function
from __future__ import unicode_literals

# In a separate file to avoid circular imports...
class Bundle( object ):
    """
    A small container for instances and converted data for the
    `dehydrate/hydrate` cycle.
    """
    def __init__( self, obj=None, data=None, request=None ):
        self.obj = obj
        self.data = data or {}
        self.new_data = False
        self.request = request
        self.errors = {}

    def __repr__( self ):
        return "<Bundle for obj='%s' with data='%s'>" % ( self.obj, self.data )

