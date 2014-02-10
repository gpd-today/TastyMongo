from __future__ import print_function
from __future__ import unicode_literals

from mongoengine import *


class Mixin(object):
    name = StringField()

    def __unicode__( self ):
        return unicode( self.name )


class Activity( Mixin, Document ):
    person = ReferenceField( 'Person', required=True )
    finished = BooleanField( default=False )
    tags = ListField( StringField() )


class Person( Mixin, Document ):
    activities = ListField( ReferenceField( 'Activity' ) )


class Deliverable( Mixin, Document ):
    owner = ReferenceField( 'Person', required=True )
    activities = ListField( ReferenceField( 'Activity' ) )



