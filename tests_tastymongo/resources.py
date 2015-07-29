


from tastymongo.constants import ALL_WITH_RELATIONS, ALL
from tastymongo.resource import DocumentResource
from tastymongo import fields

from tests_tastymongo.documents import Activity, Person, Deliverable, AllFieldsDocument


class PersonResource( DocumentResource ):

    name = fields.StringField('name')

    class Meta:
        object_class = Person
        resource_name = 'person'
        filtering = {
            'id': ['in', 'exact']
        }

class ActivityResource( DocumentResource ):

    person = fields.ToOneField( 'person', PersonResource )

    class Meta:
        object_class = Activity
        resource_name = 'activity'
        filtering = {
            'id': ['in', 'exact'],
            'name': ALL
        }

class DeliverableResource( DocumentResource ):

    owner = fields.ToOneField( 'owner', PersonResource )
    activities = fields.ToManyField( 'activities', ActivityResource )

    class Meta:
        object_class = Deliverable
        resource_name = 'deliverable'
        filtering = {
            'id': ['in', 'exact']
        }

class AllFieldsDocumentResource( DocumentResource ):

    to_one_field = fields.ToOneField( 'to_one_field', 'self' )
    to_many_field = fields.ToManyField( 'to_many_field', 'self' )

    id_field = fields.ObjectIdField( 'id_field' )
    string_field = fields.StringField( 'string_field' )
    int_field = fields.IntegerField( 'int_field' )
    float_field = fields.FloatField( 'float_field' )
    decimal_field = fields.DecimalField( 'decimal_field' )
    boolean_field = fields.BooleanField( 'boolean_field' )
    list_field = fields.ListField( 'list_field' )
    dict_field = fields.DictField( 'dict_field' )
    document_field = fields.EmbeddedDocumentField( 'document_field' )
    date_field = fields.DateField( 'date_field' )
    datetime_field = fields.DateTimeField( 'datetime_field' )
    time_field = fields.TimeField( 'time_field' )

    class Meta:
        object_class = AllFieldsDocument
        resource_name = 'all_fields_document'
        filtering = ( 'id_field', 'string_field', 'int_field', 'float_field', 'decimal_field',
        'boolean_field', 'list_field', 'dict_field', 'document_field', 'date_field', 'datetime_field', 'time_field',
        'to_one_field', 'to_many_field', 'to_one_field_not_on_resource', 'to_many_field_not_on_resource' )