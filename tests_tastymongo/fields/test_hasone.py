from __future__ import print_function
from __future__ import unicode_literals

import unittest
import json
from pyramid import testing

from tests_tastymongo.run_tests import setup_db, setup_request

from tests_tastymongo.documents import Activity, Person


class HasOneTests( unittest.TestCase ):

    def setUp( self ):
        self.conn = setup_db()
        self.data = setup_request()

    def tearDown( self ):
        testing.tearDown()

        # Clear data
        self.data = None

    def test_create_single_and_modify_nested_document( self ):
        d = self.data

        user_uri = d.person_resource.get_resource_uri( d.request, d.user )

        user = Person.objects.get( id=d.user.pk )
        self.assertEqual( user.name, 'p1' )

        d.request.body = json.dumps({
            'name': 'post_list created activity',
            'person': { 'resource_uri': user_uri, 'name': 'p2' }
        })

        response = d.activity_resource.post_list( d.request )
        deserialized = json.loads( response.body )

        self.assertEqual( deserialized['person'], user_uri )

        user = Person.objects.get( id=d.user.pk )
        self.assertEqual( user.name, 'p2' )

    def test_update_single_and_modify_nested_document( self ):
        d = self.data

        user_uri = d.person_resource.get_resource_uri( d.request, d.user )

        # Setup data
        d.a1 = Activity( name='a1', person=d.user )
        d.a1.save()

        user = Person.objects.get( id=d.user.pk )
        self.assertEqual( user.name, 'p1' )

        d.request.body = json.dumps({
            'resource_uri': d.activity_resource.get_resource_uri( d.request, d.a1 ),
            'person': { 'resource_uri': user_uri, 'name': 'p2' }
        })

        response = d.activity_resource.post_list( d.request )
        deserialized = json.loads( response.body )

        self.assertEqual( deserialized['person'], user_uri )

        user = Person.objects.get( id=d.user.pk )
        self.assertEqual( user.name, 'p2' )
