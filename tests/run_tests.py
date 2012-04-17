from __future__ import print_function
from __future__ import unicode_literals

if __name__ == '__main__' and __package__ is None:
    __package__ = 'tests'

    from .basic import tests
    tests.MyTest()