from __future__ import print_function
from __future__ import unicode_literals

from mongoengine import Document, ReferenceField
from tastymongo.fields import *


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


class AllFieldsDocument( Mixin, Document ):

    id_field = ObjectIdField()
    string_field = StringField()
    int_field = IntegerField()
    float_field = FloatField()
    decimal_field = DecimalField()
    boolean_field = BooleanField()
    list_field = ListField( StringField() )
    dict_field = DictField()
    document_field = EmbeddedDocumentField()
    date_field = DateField()
    datetime_field = DateTimeField()
    time_field = TimeField()
    to_one_field = ReferenceField( 'AllFieldsDocument' )
    to_many_field = ListField( ReferenceField( 'AllFieldsDocument' ) )