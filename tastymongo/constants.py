from __future__ import print_function
from __future__ import unicode_literals

# Enable all basic ORM filters but do not allow filtering across relationships.
ALL = 1
# Enable all ORM filters, including across relationships
ALL_WITH_RELATIONS = 2


# FIXME: Query terms should be exposed by MongoEngine like in Django.
# Valid query terms:
QUERY_OPERATORS = ['ne', 'size', 'exists', 'exact']
QUERY_INEQUALITY_OPERATORS = [ 'gt', 'gte', 'lt', 'lte' ]
QUERY_ITERABLE_OPERATORS = [ 'in', 'nin', 'all' ]
# QUERY_GEO_OPERATORS = ['within_distance', 'within_spherical_distance', 'within_box', 'within_polygon', 'near', 'near_sphere']
QUERY_MATCH_OPERATORS = ['contains', 'icontains', 'startswith', 'istartswith', 'endswith', 'iendswith', 'iexact']

# Use a Set for O(1) lookups and because filters are also specified dictwise
QUERY_TERMS = set( QUERY_OPERATORS ) | set( QUERY_INEQUALITY_OPERATORS ) | set( QUERY_ITERABLE_OPERATORS ) | set( QUERY_MATCH_OPERATORS )

LOOKUP_SEP = '__'