from __future__ import print_function
from __future__ import unicode_literals

from pyramid.config import Configurator
from pyramid.request import Request
from pyramid.response import Response

from tastymongo.resource import Resource


def add_route( pattern, view, name ):
    pass

def register_resource(config, resource):
    resource_name = resource._meta.resource_name

    # add 'list' action
    config.add_route('{}_list'.format(resource_name), '/{}/'.format(resource_name))
    config.add_view(resource.dispatch_list, route_name='{}_list'.format(resource_name))

    # add 'schema' action
    config.add_route('{}_get_schema'.format(resource_name), '/{}/schema/'.format(resource_name))
    config.add_view(resource.get_schema, route_name='{}_get_schema'.format(resource_name))

    # add 'get_multiple' action
    config.add_route('{}_get_multiple'.format(resource_name), '/{}/set/{{ids}}/'.format(resource_name))
    config.add_view(resource.get_multiple, route_name='{}_get_multiple'.format(resource_name))

    # add 'detail' action
    config.add_route('{}_detail'.format(resource_name), '/{}/{{id}}/'.format(resource_name))
    config.add_view(resource.dispatch_detail, route_name='{}_detail'.format(resource_name))


def resolve_uri( uri='/' ):
    pass

def build_uri( request, route_name, *elements ):
    return request.route_url( route_name, *elements )
