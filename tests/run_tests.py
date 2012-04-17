from __future__ import print_function
from __future__ import unicode_literals

import mongoengine

import unittest

if __name__ == '__main__':
    mongoengine.register_connection( mongoengine.DEFAULT_CONNECTION_NAME, 'TastyMongo_test' )

    test_loader = unittest.defaultTestLoader.discover( '.' )
    test_runner = unittest.TextTestRunner()
    test_runner.run( test_loader )
