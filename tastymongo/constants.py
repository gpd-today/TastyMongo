from __future__ import print_function
from __future__ import unicode_literals

# Enable all basic ORM filters but do not allow filtering across relationships.
ALL = 1
# Enable all ORM filters, including across relationships
ALL_WITH_RELATIONS = 2


# FIXME: Query terms should be exposed by MongoEngine like in Django.
# Valid query terms.
QUERY_OPERATORS = ['ne', 'gt', 'gte', 'lt', 'lte', 'in', 'nin', 'mod',
             'all', 'size', 'exists', 'not']
QUERY_GEO_OPERATORS = ['within_distance', 'within_spherical_distance', 'within_box', 'within_polygon', 'near', 'near_sphere']
QUERY_MATCH_OPERATORS = ['contains', 'icontains', 'startswith',
                   'istartswith', 'endswith', 'iendswith',
                   'exact', 'iexact']
QUERY_CUSTOM_OPERATORS = ['match']

# Use a Dict for O(1) lookups and because filters are also specified dictwise
QUERY_TERMS = dict([(x, None) for x in (QUERY_OPERATORS + QUERY_GEO_OPERATORS + QUERY_MATCH_OPERATORS + QUERY_CUSTOM_OPERATORS)])

LOOKUP_SEP = '__'
