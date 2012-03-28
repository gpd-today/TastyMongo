from __future__ import print_function
from __future__ import unicode_literals

from pyramid.config import Configurator
from tastymongo.resource import Resource


def add_route( pattern, view, name ):
    pass

def register_resource( resource, routes ):
    var name = resource._meta.resource_name
    pass

def resolve_uri( uri='/' ):
    pass

def build_uri( resource ):
    pass
