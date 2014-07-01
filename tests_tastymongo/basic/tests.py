from __future__ import print_function
from __future__ import unicode_literals

# from mongoengine_relational import RelationManagerMixin
import unittest
import json
from pyramid import testing

from pyramid.request import Request

from tests_tastymongo.run_tests import setup_db, setup_request
from tests_tastymongo.documents import Activity, Person, Deliverable
from tastymongo import http


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

    def test_put_single( self ):
        d = self.data

        deliverable = Deliverable( name='d1', owner=d.user )
        deliverable.save()

        activity = Activity( person=d.user, name='a2' )
        activity.save()

        deliverable.activities.append( activity )
        deliverable.save()

        self.assertEqual( len( deliverable.activities ), 1 )

        # Change the deliverable's name to `d2`
        request = Request.blank( d.deliverable_resource.get_resource_uri( d.request, deliverable ) )
        request.body = json.dumps({
            'name': 'd2',
            'id': str( deliverable.pk ),
            'resource_uri': d.deliverable_resource.get_resource_uri( d.request, deliverable ),
            'owner': d.person_resource.get_resource_uri( d.request, d.user )
        })

        # Update the deliverable
        response = d.deliverable_resource.put_single( request )
        deserialized = json.loads( response.body )

        deliverable.reload()

        self.assertEqual( deliverable.name, 'd2' )
        self.assertEqual( len( deliverable.activities ), 1 )

        # Change the name again, this time to `d3`
        request = Request.blank( d.deliverable_resource.get_resource_uri( d.request, deliverable ) )
        request.body = json.dumps({
            'name': 'd3',
            'id': str( deliverable.pk ),
            'resource_uri': d.deliverable_resource.get_resource_uri( d.request, deliverable )
        })

        # Update the deliverable
        response = d.deliverable_resource.put_single( request )
        deliverable.reload()

        self.assertEqual( deliverable.name, 'd3' )
        self.assertEqual( len( deliverable.activities ), 1 )


    def test_post_nested_list( self ):
        d = self.data

        d.request.body = json.dumps({
            'name': 'post_list created activity',
            'person': {
                'name': 'nested person'
            }
        })

        # we want to return the nested person as well:
        d.activity_resource.fields[ 'person' ].full = True

        # Create a new activity
        response = d.activity_resource.post_list( d.request )
        deserialized = json.loads( response.body )

        self.assertIn( 'id', deserialized )
        self.assertEqual( deserialized['name'], 'post_list created activity' )
        person = deserialized[ 'person' ]
        self.assertEqual( person['name'], 'nested person' )

        # FIXME: the following fails, because in dispatch_single the nested person field does not get dereferenced
        # and this breaks the dehydration of the person field. We could set _auto_dereference=True, but we don't know
        # what else this impacts. This fails irrespective of importing RelationalManagerMixin.
        # # Find out if the activity was indeed created:
        d.request.matchdict = { 'name': 'post_list created activity'}
        response = d.activity_resource.dispatch_single( d.request )
        deserialized = json.loads( response.body )

        # Check if the correct activity has been returned
        self.assertEqual( deserialized['name'], 'post_list created activity' )

        # Check if the nested person has been returned properly:
        person = deserialized[ 'person' ]
        self.assertEqual( person['name'], 'nested person' )


        # Now we post the same activity, with the same person nested in it, and change the name fields on them. In this
        # way, we test whether the fields and specifically the related field's fields are correctly dehydrated and saved
        d.request.body = json.dumps({
            'id': str( deserialized['id'] ),
            'resource_uri': '/api/v1/person/' + str( deserialized['id'] ) + '/',
            'name': 'new name activity',
            'person': {
                'id': str( person['id'] ),
                'resource_uri': '/api/v1/person/' + str( person['id'] ) + '/',
                'name': 'new name'
            }
        })

        response = d.activity_resource.post_list( d.request )
        deserialized_2 = json.loads( response.body )

        self.assertIn( 'id', deserialized_2 )
        self.assertEqual( deserialized['id'], deserialized_2['id'] )
        self.assertEqual( deserialized_2['name'], 'new name activity' )
        person_2 = deserialized_2[ 'person' ]
        self.assertEqual( person_2['name'], 'new name' )
        self.assertEqual( person['id'], person_2['id'] )