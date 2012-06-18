from __future__ import print_function
from __future__ import unicode_literals

from tastymongo.resource import DocumentResource
from tastymongo import fields

from .documents import Activity, Person

class ActivityResource( DocumentResource ):

    name = fields.StringField('name')
    person = fields.ToOneField(
            'basic.resources.PersonResource',
            'person', )

    class Meta:
        resource_name = 'activity'
        queryset = Activity.objects


class PersonResource( DocumentResource ):

    name = fields.StringField('name')

    class Meta:
        resource_name = 'person'
        queryset = Person.objects
