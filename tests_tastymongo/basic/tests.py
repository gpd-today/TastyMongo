from __future__ import print_function
from __future__ import unicode_literals

import unittest
import json
import mongoengine

from bson import DBRef, ObjectId

from pyramid import testing
from pyramid.request import Request

from tests_tastymongo.documents import Activity, Person
from tests_tastymongo.resources import ActivityResource, PersonResource, DeliverableResource
from tests_tastymongo.utils import Struct

from tastymongo.api import Api


class DetailTests( unittest.TestCase ):

    def setUp( self ):
        # Setup application/request config
        self.request = Request.blank( '/api/v1/' )
        self.config = testing.setUp( request=self.request )

        # Setup our API
        self.api = Api( self.config )

        # Create some resources
        self.activity_resource = ActivityResource()
        self.deliverable_resource = DeliverableResource()
        self.person_resource = PersonResource()
        self.api.register( self.activity_resource )
        self.api.register( self.person_resource )

        # Setup data
        d = self.data = Struct()
        d.person1, created = Person.objects.get_or_create( name='Dude', defaults={ 'email': 'dude@progressivecompany.com', 'password': 'dude' } )
        d.person2, created = Person.objects.get_or_create( name='Dude2', defaults={ 'email': 'dude2@progressivecompany.com', 'password': 'dude' } )
        d.activity1, created = Activity.objects.get_or_create( name='Act1!', person=d.person1 )
        d.activity2, created = Activity.objects.get_or_create( name='Act2!', person=d.person1 )

        self.request.user = d.person1
        policy = self.config.testing_securitypolicy( userid=str( d.person1.pk ) ) #, permissive=True )
        self.config.set_authentication_policy( policy )

    def tearDown( self ):
        testing.tearDown()

        # Clear our references
        self.data = None

    def test_get_single( self ):
        # Get a single activity
        self.request.matchdict = { 'id': self.data.activity1.id }
        response = self.activity_resource.dispatch_single( self.request )
        deserialized = json.loads( response.body )

        # Check if the correct activity has been returned
        self.assertEqual( deserialized['id'], unicode(self.data.activity1.id) )
        self.assertEqual( deserialized['person'].split('/')[-2], unicode(self.data.person1.id) )

    def test_get_list( self ):
        # Get a bunch of activities
        self.request.matchdict = {}
        response = self.activity_resource.dispatch_list( self.request )
        deserialized = json.loads( response.body )

        # Find out if we got multiple activities
        self.assertEqual( len(deserialized['objects']), deserialized['meta']['total_count'] )

    def test_post_list( self ):
        self.request.body = b'{{ "name": "post_list created activity", "person": "/api/v1/person/{0}/"}}'.format( self.data.person1.pk )

        # Create a new activity
        response = self.activity_resource.post_list( self.request )
        deserialized = json.loads( response.body )
        print( response )
        self.assertIn( 'id', deserialized )

        # Find out if it was indeed created:
        self.request.matchdict = { 'name': 'post_list created activity'}
        response = self.activity_resource.dispatch_single( self.request )
        print( response )
        deserialized = json.loads( response.body )

        # Check if the correct activity has been returned
        self.assertEqual( deserialized['name'], "post_list created activity")
