from __future__ import print_function
from __future__ import unicode_literals

from mongoengine import Document, EmbeddedDocument, ReferenceField
from mongoengine.fields import *


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

class EmbeddedDoc( EmbeddedDocument ):

     id_field = ObjectIdField()

class AllFieldsDocument( Mixin, Document ):

    id_field = ObjectIdField()
    string_field = StringField()
    int_field = IntField()
    float_field = FloatField()
    decimal_field = DecimalField()
    boolean_field = BooleanField()
    list_field = ListField( StringField() )
    dict_field = DictField()
    document_field = EmbeddedDocumentField( 'EmbeddedDoc' )
    date_field = DateTimeField()
    datetime_field = DateTimeField()
    time_field = DateTimeField()
    to_one_field = ReferenceField( 'AllFieldsDocument' )
    to_many_field = ListField( ReferenceField( 'AllFieldsDocument' ) )