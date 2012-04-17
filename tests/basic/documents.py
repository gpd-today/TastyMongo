from __future__ import print_function
from __future__ import unicode_literals

from mongoengine import *

class Activity( Document ):
    name = StringField( required=True )
    person = ReferenceField( 'Person', required=True )
    finished = BooleanField( default=False )