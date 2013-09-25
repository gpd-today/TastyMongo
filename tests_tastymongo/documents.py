from __future__ import print_function
from __future__ import unicode_literals

from mongoengine import *


class Activity( Document ):

    name = StringField( required=True )
    person = ReferenceField( 'Person', required=True )
    finished = BooleanField( default=False )
    tags = ListField( StringField() )

    def __unicode__( self ):
        return unicode( self.name )


class Person( Document ):

    name = StringField( required=True )
    activities = ListField( ReferenceField( 'Activity' ) )

    def __unicode__( self ):
        return unicode( self.name )


class Deliverable( Document ):

    name = StringField( required=True )
    owner = ReferenceField( 'Person', required=True )
    activities = ListField( ReferenceField( 'Activity' ) )

    def __unicode__( self ):
        return unicode( self.name )
