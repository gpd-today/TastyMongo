from __future__ import print_function
from __future__ import unicode_literals

import unittest
import json
import mongoengine
from pyramid import testing

from tests_tastymongo.run_tests import setup_db, setup_request

from tests_tastymongo.documents import Activity, Person, Deliverable


class BasicTests( unittest.TestCase ):

    def setUp( self ):
        self.conn = setup_db()
        self.data = setup_request()

        # Setup data
        self.data.a1 = Activity( name='a1', person=self.data.user )
        self.data.a1.save()


    def tearDown( self ):
        testing.tearDown()

        # Clear data
        self.data = None

    def test_document_to_uri( self ):
        d = self.data

        uri = d.person_resource.get_resource_uri( d.request, d.user )
        self.assertEqual( uri, '/api/v1/person/{0}/'.format( d.user.pk ) )

        uri = d.activity_resource.get_resource_uri( d.request, d.a1 )
        self.assertEqual( uri, '/api/v1/activity/{0}/'.format( d.a1.pk ) )

        a2 = Activity( name='a2', person=d.user )
        uri = d.activity_resource.get_resource_uri( d.request, a2 )
        self.assertEqual( uri, '/api/v1/activity/None/' ) # TODO: not sure this is correct. Would the list uri be better?

    def test_get_single( self ):
        d = self.data

        # Get a single activity
        d.request.matchdict = { 'id': self.data.a1.id }
        response = d.activity_resource.dispatch_single( d.request )
        deserialized = json.loads( response.body )

        # Check if the correct activity has been returned
        self.assertEqual( deserialized['id'], unicode(self.data.a1.id) )
        self.assertEqual( deserialized['person'].split('/')[-2], unicode(self.data.user.id) )

    def test_get_list( self ):
        d = self.data

        # Get a bunch of activities
        d.request.matchdict = {}
        response = d.activity_resource.dispatch_list( d.request )
        deserialized = json.loads( response.body )

        # Find out if we got multiple activities
        self.assertEqual( len(deserialized['objects']), deserialized['meta']['total_count'] )

    def test_post_list( self ):
        d = self.data

        d.request.body = json.dumps({
            'name': 'post_list created activity',
            'person': d.person_resource.get_resource_uri( d.request, d.user )
        })

        # Create a new activity
        response = d.activity_resource.post_list( d.request )
        deserialized = json.loads( response.body )
        self.assertIn( 'id', deserialized )

        # Find out if it was indeed created:
        d.request.matchdict = { 'name': 'post_list created activity'}
        response = d.activity_resource.dispatch_single( d.request )
        deserialized = json.loads( response.body )

        # Check if the correct activity has been returned
        self.assertEqual( deserialized['name'], "post_list created activity")
