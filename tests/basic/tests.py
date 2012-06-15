from __future__ import print_function
from __future__ import unicode_literals

import unittest
import json

from pyramid import testing
from pyramid.request import Request

from .documents import Activity, Person
from .resources import ActivityResource, PersonResource

from tastymongo.api import Api
from mongoengine.base import ValidationError


class db_proxy:
    # an empty class to stuff references
    pass


class DetailTests( unittest.TestCase ):

    def setup_test_data( self ):

        # Index our database objects
        self.data = db_proxy()
        self.data.person1, created = Person.objects.get_or_create( name='Dude', defaults={ 'email': 'dude@progressivecompany.com', 'password': 'dude' } )
        self.data.person2, created = Person.objects.get_or_create( name='Dude2', defaults={ 'email': 'dude2@progressivecompany.com', 'password': 'dude' } )
        self.data.activity1, created = Activity.objects.get_or_create( name='Act1!', person=self.data.person1 )
        self.data.activity2, created = Activity.objects.get_or_create( name='Act2!', person=self.data.person1 )

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
        self.person_resource = PersonResource()
        self.api.register( self.activity_resource )
        self.api.register( self.person_resource )

        self.setup_test_data()

    def tearDown( self ):
        self.teardown_test_data()
        testing.tearDown()

    def test_get_detail( self ):
        request = Request.blank('/api/v1/')
        self.config.testing_securitypolicy( userid='1', permissive=True )
        request.user = self.data.person1

        # Get a single activity
        request.matchdict = { 'id': self.data.activity1.id }
        response = self.activity_resource.dispatch_detail( request )
        deserialized = json.loads( response.body )

        # Check if the correct activity has been returned
        self.assertEqual( deserialized['id'], unicode(self.data.activity1.id) )

        # Check if the activity contains the person
        self.assertEqual( deserialized['person'].split('/')[-2], unicode(self.data.person1.id) )

    def test_post_list( self ):
        request = Request.blank( '/api/v1/' )
        self.config.testing_securitypolicy( userid='1', permissive=True )

        request.body = b'{{ "name": "post_list created activity", "person": "/api/v1/person/{0}/"}}'.format(self.data.person1.id)

        # Create a new activity
        response = self.activity_resource.post_list( request )

