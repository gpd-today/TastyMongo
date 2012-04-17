from __future__ import print_function
from __future__ import unicode_literals

import unittest
from pyramid import testing
from pyramid.response import Response

from .documents import Activity
from .resources import ActivityResource

from tastymongo.api import Api

class MyTest( unittest.TestCase ):
    def setUp( self ):
        self.config = testing.setUp()

        #self.api = Api( self.config )
        self.activity_resource = ActivityResource()
        #self.api.register( self.activity_resource )


    def tearDown( self ):
        testing.tearDown()

    def test_activity( self ):
        request = testing.DummyRequest()
        request.context = testing.DummyResource()

        response = self.activity_resource.dispatch_list( request )

        self.assertTrue( isinstance( response, Response ) )