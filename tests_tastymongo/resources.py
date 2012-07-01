from __future__ import print_function
from __future__ import unicode_literals

from tastymongo.resource import DocumentResource
from tastymongo import fields

from tests_tastymongo.documents import Activity, Person


class PersonResource( DocumentResource ):

    name = fields.StringField('name')

    class Meta:
        resource_name = 'person'
        queryset = Person.objects


class ActivityResource( DocumentResource ):

    name = fields.StringField('name')
    person = fields.ToOneField( PersonResource, 'person', )

    class Meta:
        resource_name = 'activity'
        queryset = Activity.objects


