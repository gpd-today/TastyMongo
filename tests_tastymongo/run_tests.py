from __future__ import print_function
from __future__ import unicode_literals

import unittest

import mongoengine

from pyramid import testing
from pyramid.request import Request

from tastymongo.api import Api

from tests_tastymongo.utils import Struct
from tests_tastymongo.documents import Person
from tests_tastymongo.resources import ActivityResource, PersonResource, DeliverableResource


def setup_db( drop=True ):
    mongoengine.register_connection( mongoengine.DEFAULT_CONNECTION_NAME, 'tastymongo_test' )
    c = mongoengine.connection.get_connection()

    if drop:
        c.drop_database( 'tastymongo_test' )

    return c

def setup_request( user=None ):
    d = Struct()

    # Setup application/request config
    d.request = Request.blank( '/api/v1/' )
    d.config = testing.setUp( request=d.request )

    # Setup our API
    d.api = Api( d.config )

    # Create some resources
    d.activity_resource = ActivityResource()
    d.deliverable_resource = DeliverableResource()
    d.person_resource = PersonResource()
    d.api.register( d.activity_resource )
    d.api.register( d.deliverable_resource )
    d.api.register( d.person_resource )

    if user is not False:
        if user:
            d.user = user
        else:
            d.user = Person( name='p1', defaults={ 'email': 'p1@progressivecompany.com', 'password': 'p1' } )
            d.user.save()

        d.request.user = d.user
        policy = d.config.testing_securitypolicy( userid=str( d.user.pk ) ) #, permissive=True )
        d.config.set_authentication_policy( policy )

    return d

if __name__ == '__main__':
    test_loader = unittest.defaultTestLoader.discover( '.' )
    test_runner = unittest.TextTestRunner()
    test_runner.run( test_loader )