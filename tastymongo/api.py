from __future__ import print_function
from __future__ import unicode_literals

import warnings

from pyramid.response import Response
from pyramid.config import Configurator

from .exceptions import NotRegistered, BadRequest, ConfigurationError
from .serializers import Serializer
from .utils import *


class Api(object):
    """
    Implements a registry to tie together the various resources that make up
    an API.

    Especially useful for navigation, HATEOAS and for providing multiple
    versions of your API.

    Optionally supplying ``api_name`` allows you to name the API. Generally,
    this is done with version numbers (i.e. ``v1``, ``v2``, etc.) but can
    be named any string.
    """
    def __init__(self, config, api_name='api', api_version='v1'):
        self.api_name = api_name
        self.api_version = api_version
        self._registry = {}
        self.config = config

        self.route = '/{}/{}'.format(self.api_name, self.api_version)

        self.config.add_route(self.route, self.route + '/')
        self.config.add_view(self.top_level, route_name=self.route)

    @staticmethod
    def resolve_uri( uri='/' ):
        pass

    @staticmethod
    def build_uri( request, route_name, *elements ):
        return request.route_path( route_name, *elements )

    def build_route_name(self, resource_name, operation):
        if resource_name is not None:
            route_name = '{}/{}/{}'.format(self.route, resource_name, operation)
        else:
            route_name = '{}/{}'.format(self.route, operation)

        return route_name

    def register(self, resource):
        """
        Registers an instance of a ``Resource`` subclass with the API.
        """
        resource_name = getattr(resource._meta, 'resource_name', None)

        if resource_name is None:
            raise ConfigurationError("Resource %r must define a 'resource_name'." % resource)

        self._registry[resource_name] = resource

        # TODO: This is messy, but makes URI resolution on FK/M2M fields work consistently.
        resource._meta.api_name = self.api_name
        resource.__class__.Meta.api_name = self.api_name

        # add 'list' action
        list_name = self.build_route_name(resource_name, 'list')
        self.config.add_route(list_name, '{}/{}/'.format(self.route, resource_name))
        self.config.add_view(resource.dispatch_list, route_name=list_name)

        # add 'schema' action
        schema_name = self.build_route_name(resource_name, 'schema')
        self.config.add_route(schema_name, '{}/{}/schema'.format(self.route, resource_name))
        self.config.add_view(resource.get_schema, route_name=schema_name)

        # add 'get_multiple' action
        multiple_name = self.build_route_name(resource_name, 'multiple')
        self.config.add_route(multiple_name, '{}/{}/set/{{ids}}/'.format(self.route, resource_name))
        self.config.add_view(resource.get_multiple, route_name=multiple_name)

        # add 'detail' action
        detail_name = self.build_route_name(resource_name, 'detail')
        self.config.add_route(detail_name, '{}/{}/{{id}}/'.format(self.route, resource_name))
        self.config.add_view(resource.dispatch_detail, route_name=detail_name)

    def unregister(self, resource_name):
        """
        If present, unregisters a resource from the API.
        """
        if resource_name in self._registry:
            del(self._registry[resource_name])
        else:
            raise NotRegistered("No resource was registered for '%s'." % resource_name)

    def wrap_view(self, view):
        def wrapper(request, *args, **kwargs):
            return getattr(self, view)(request, *args, **kwargs)
        return wrapper

    def top_level(self, request):
        """
        A view that returns a serialized list of all resources registers
        to the ``Api``. Useful for discovery.
        """
        serializer = Serializer()
        available_resources = {}

        for resource_name in sorted(self._registry.keys()):
            available_resources[resource_name] = {
                'list_endpoint': self.build_uri(request, self.build_route_name(resource_name, 'list')),
                'schema': self.build_uri(request, self.build_route_name(resource_name, 'schema')),
            }

        desired_format = determine_format(request, serializer)
        options = {}

        serialized = serializer.serialize(available_resources, desired_format, options)
        return Response( body=serialized, content_type=build_content_type(desired_format) )
