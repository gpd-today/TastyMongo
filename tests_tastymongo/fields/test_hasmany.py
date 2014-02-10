from __future__ import print_function
from __future__ import unicode_literals

import unittest
import json
import mongoengine
from pyramid import testing

from tests_tastymongo.run_tests import setup_db, setup_request

from tests_tastymongo.documents import Activity, Person, Deliverable


class HasOneTests( unittest.TestCase ):

    def setUp( self ):
        self.conn = setup_db()
        self.data = setup_request()

    def tearDown( self ):
        testing.tearDown()

        # Clear data
        self.data = None