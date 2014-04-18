from __future__ import print_function
from __future__ import unicode_literals

import unittest
from pyramid import testing

from tests_tastymongo.run_tests import setup_db, setup_request


class HasOneTests( unittest.TestCase ):

    def setUp( self ):
        self.conn = setup_db()
        self.data = setup_request()

    def tearDown( self ):
        testing.tearDown()

        # Clear data
        self.data = None
