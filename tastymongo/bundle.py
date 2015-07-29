


from mongoengine import Document

# In a separate file to avoid circular imports...
class Bundle( object ):
    """
    A small container for instances and converted data for the
    `dehydrate/hydrate` cycle.
    """
    def __init__( self, obj=None, data=None, request=None ):
        if isinstance( obj, Document ) and hasattr( request, 'cache' ) and obj.pk:
            obj = request.cache.add( obj )

        self.obj = obj
        self.data = data or {}
        self.uri_only = True
        self.request = request

    def __repr__( self ):
        return '<Bundle for obj=`{}` with data=`{}`>'.format( self.obj, self.data )

