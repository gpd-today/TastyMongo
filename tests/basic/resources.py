from __future__ import print_function
from __future__ import unicode_literals

from tastymongo.resource import DocumentResource
from tastymongo import fields

from .documents import Activity

class ActivityResource( DocumentResource ):

    class Meta:
        resource_name = 'activity'
        queryset = Activity.objects