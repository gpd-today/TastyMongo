from __future__ import print_function
from __future__ import unicode_literals

from tastymongo.resource import DocumentResource
from tastymongo import fields

from tests_tastymongo.documents import Activity, Person


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
            'id': ['in', 'exact']
        }
