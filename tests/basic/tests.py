from __future__ import print_function
from __future__ import unicode_literals

import unittest

from pyramid import testing
from pyramid.response import Response
from pyramid.request import Request

from .documents import Activity, Person
from .resources import ActivityResource, PersonResource

from tastymongo.api import Api

try: 
    import simplejson as json
except ImportError: 
    import json

class MyTest( unittest.TestCase ):

    def setup_test_data( self ):

        class db_proxy:
            # an empty class to put stuff on for easier referencing
            pass

        # Create an index to our database objects
        db = self.proxy = db_proxy()
        db.person, created = Person.objects.get_or_create( name='Dude', defaults={ 'email': 'dude@progressivecompany.com', 'password': 'dude' } )
        db.activity, created = Activity.objects.get_or_create( name='Act!', person=self.proxy.person )

    def teardown_test_data( self ):

        # Clear the database
        Person.drop_collection()
        Activity.drop_collection()

        # Clear our references
        self.proxy = None

    def setUp( self ):

        # Setup our API
        self.config = testing.setUp()
        self.api = Api( self.config )

        # Create some resources
        self.activity_resource = ActivityResource()
        self.api.register( self.activity_resource )

        self.setup_test_data()

    def tearDown( self ):
        self.teardown_test_data()
        testing.tearDown()

    def test_get_detail( self ):
        request = Request.blank('/api/v1/')

        # Get a single activity
        request.matchdict = { 'id': self.proxy.activity.id }
        response = self.activity_resource.dispatch_detail( request )
        parsed = json.loads( response.body )

        # Check if the correct activity has been returned
        self.assertEqual( parsed['id'], unicode(self.proxy.activity.id) )

