from __future__ import print_function
from __future__ import unicode_literals

import unittest

from pyramid import testing

from tastymongo.constants import *
from tastymongo.exceptions import InvalidFilterError

from bson import ObjectId
from tests_tastymongo.documents import AllFieldsDocument, EmbeddedDoc
from tests_tastymongo.resources import AllFieldsDocumentResource
from tests_tastymongo.run_tests import setup_db, setup_request

from decimal import Decimal
import datetime


class BasicTests( unittest.TestCase ):
    """
    Given TastyMongo's set of fields and allowed query operators, there are plenty of different filtering possibilities.
    The combination between field type and filter operator type, we can infer to what type we should parse the filter
    value. These combinations are listed below, where an 'x' denotes that this combination is not possible (sensible).
    The tests in this class cover this entire table, seeing to it that we parse to the right output whenever possible,
    or reject the filter when it cannot be made sensible.

    +-----------------------+----------------+------------------+----------------+-----------------+---------+------+
    |                       |   exact, ne    | gt, gte, lt, lte |  in, nin, all  | MATCH_OPERATORS | exists  | size |
    +-----------------------+----------------+------------------+----------------+-----------------+---------+------+
    | ObjectIdField         | ObjectId       | ObjectId         | List<ObjectId> | x               | Boolean | x    |
    | StringField           | String         | String           | List<String>   | String          | Boolean | x    |
    | IntegerField          | Int            | Int              | List<Int>      | x               | Boolean | x    |
    | FloatField            | Float          | Float            | List<Float>    | x               | Boolean | x    |
    | DecimalField          | Decimal        | Decimal          | List<Decimal>  | x               | Boolean | x    |
    | BooleanField          | Boolean        | Boolean          | List<Boolean>  | x               | Boolean | x    |
    | ListField             | x              | x                | x              | x               | Boolean | Int  |
    | DictField             | x              | x                | x              | x               | Boolean | Int  |
    | EmbeddedDocumentField | x              | x                | x              | x               | Boolean | Int  |
    | DateField             | Date           | Date             | List<Date>     | x               | Boolean | x    |
    | DateTimeField         | DateTime       | DateTime         | List<DateTime> | x               | Boolean | x    |
    | TimeField             | Time           | Time             | List<Time>     | x               | Boolean | x    |
    | ToOneField            | ObjectId       | ObjectId         | List<ObjectId> | x               | Boolean | x    |
    | ToManyField           | List<ObjectId> | x                | List<ObjectId> | x               | Boolean | Int  |
    +-----------------------+----------------+------------------+----------------+-----------------+---------+------+

    """
    # TODO: Date / Datetime / Timefield filter testing

    def setUp( self ):
        self.conn = setup_db()
        self.data = setup_request()

        # Insert a document
        self.data.document = AllFieldsDocument(
            id_field = ObjectId(),
            string_field = 'hello world',
            int_field = 4,
            float_field = 4.5,
            decimal_field = Decimal( 4 / 3 ),
            boolean_field = True,
            list_field = [ 'hello', 'world' ],
            dict_field = { 'hello': 'world' },
            document_field = EmbeddedDoc(),
            date_field = datetime.date.today(),
            datetime_field = datetime.datetime.today(),
            time_field = datetime.datetime.today(),
            to_one_field = None,
            to_many_field = None
        )
        # we need to save before we can set a recursive relation:
        self.data.document.save()
        self.data.document.to_one_field = self.data.document
        self.data.document.to_many_field = [ self.data.document ]
        self.data.document.to_one_field_not_on_resource = self.data.document
        self.data.document.to_many_field_not_on_resource = [ self.data.document ]
        self.data.document.save()

        # the api url is needed to parse resource_uris
        self.data.api_url = self.data.allfieldsdocument_resource._meta.api.route

        self.data.resource = AllFieldsDocumentResource()

        # all tastymongo fields:
        self.data.document_fields = { 'id_field', 'string_field', 'int_field', 'float_field', 'decimal_field',
        'boolean_field', 'list_field', 'dict_field', 'document_field', 'date_field', 'datetime_field', 'time_field',
        'to_one_field', 'to_many_field', 'to_one_field_not_on_resource', 'to_many_field_not_on_resource' }


    def tearDown( self ):
        testing.tearDown()

        # Clear data
        self.data = None


    def test_match_operator_filter( self ):
        """
        Match operators only work on strings, so proper filters should be built only for string fields
        """

        disallowed_fields = self.data.document_fields - { 'string_field' }

        for filter_type in QUERY_MATCH_OPERATORS:

            # check that the filtering gets rejected for fields that are not string fields:
            for field in disallowed_fields:
                with self.assertRaises( InvalidFilterError ):
                    q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: None }, None )

            # and that we get a decent q_filter for string fields:
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'string_field' + '__' + filter_type: 'hello world' }, None )
            self.assertDictEqual( q_filter.query, { 'string_field__' + filter_type: 'hello world' } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_size_filter( self ):
        """
        The size operator only works on list, dict, embeddeddocument and tomany fields, so test that we throw an error
        otherwise
        """

        allowed_fields = { 'list_field', 'dict_field', 'document_field', 'to_many_field' }
        disallowed_fields = self.data.document_fields - allowed_fields

        # check that the filtering gets rejected for fields that are not allowed:
        for field in disallowed_fields:
            with self.assertRaises( InvalidFilterError ):
                q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__size': 5 }, None )

        # and that we get a decent query for allowed fields:
        for field in allowed_fields:
            q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__size': 5 }, None )
            self.assertDictEqual( q_filter.query, { field + '__size': 5 } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_objectid_and_to_one_field_filter( self ):
        object_id = ObjectId()

        for field in ( 'id_field', 'to_one_field' ):

            for filter_type in QUERY_EQUALITY_OPERATORS:

                value = str( object_id )
                q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: value }, None )
                self.assertDictEqual( q_filter.query, { field + '__' + filter_type: object_id } )
                result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

                value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
                q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: value }, None )
                self.assertDictEqual( q_filter.query, { field + '__' + filter_type: object_id } )
                result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

                if field == 'to_one_field':
                    value = 'null'
                    q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: value }, None )
                    self.assertDictEqual( q_filter.query, { field + '__' + filter_type: None } )
                    result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

                value = 'should fail'
                with self.assertRaises( InvalidFilterError ):
                    q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: value }, None )

            for filter_type in QUERY_LIST_OPERATORS:

                value = str( object_id )
                q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: value }, None )
                self.assertDictEqual( q_filter.query, { field + '__' + filter_type: [ object_id ] } )
                result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

                value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
                q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: value }, None )
                self.assertDictEqual( q_filter.query, { field + '__' + filter_type: [ object_id ] } )
                result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

                value = 'should fail'
                with self.assertRaises( InvalidFilterError ):
                    q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: value }, None )

                id_list = [ ObjectId(), ObjectId() ]
                stringed_id_list = [ str( value ) for value in id_list ]
                q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: stringed_id_list }, None )
                self.assertDictEqual( q_filter.query, { field + '__' + filter_type: id_list } )
                result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_string_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            # we pick the string 'None' which should not be recognized as anything but a string
            value = 'None'

            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'string_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'string_field__' + filter_type: value } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

        for filter_type in QUERY_LIST_OPERATORS:

            value = 'None'

            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'string_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'string_field__' + filter_type: [ value ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_int_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            for value in ( -1.9, -1.2, -1, 0, 0.0, 3, 3.2, 3.9, 3.4999999999999999999999999999999999999999999 ):
                q_filter = self.data.allfieldsdocument_resource.build_filters( { 'int_field__' + filter_type: str( value ) }, None )
                self.assertDictEqual( q_filter.query, { 'int_field__' + filter_type: round( value ) } )
                result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

        for filter_type in QUERY_LIST_OPERATORS:

            value = 3
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'int_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( q_filter.query, { 'int_field__' + filter_type: [ value ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_float_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            for value in ( -1.9, -1.2, -1, 0, 0.0, 3, 3.2, 3.9, 3.4999999999999999999999999999999999999999999 ):
                q_filter = self.data.allfieldsdocument_resource.build_filters( { 'float_field__' + filter_type: str( value ) }, None )
                self.assertDictEqual( q_filter.query, { 'float_field__' + filter_type: value } )
                result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

        for filter_type in QUERY_LIST_OPERATORS:

            value = 3.9
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'float_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( q_filter.query, { 'float_field__' + filter_type: [ value ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_decimal_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            value = Decimal( 4.8 )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'decimal_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( q_filter.query, { 'decimal_field__' + filter_type: value } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

        for filter_type in QUERY_LIST_OPERATORS:

            value = Decimal( 4.9 )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'decimal_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( q_filter.query, { 'decimal_field__' + filter_type: [ value ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_boolean_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            value = True
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'boolean_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( q_filter.query, { 'boolean_field__' + filter_type: value } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = False
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'boolean_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( q_filter.query, { 'boolean_field__' + filter_type: value } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'boolean_field__' + filter_type: 'null' }, None )
            self.assertDictEqual( q_filter.query, { 'boolean_field__' + filter_type: None } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

        for filter_type in QUERY_LIST_OPERATORS:

            value = True
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'boolean_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( q_filter.query, { 'boolean_field__' + filter_type: [ value ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_to_one_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            object_id = ObjectId()

            value = str( object_id )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field__' + filter_type: object_id } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field__' + filter_type: object_id } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = 'null'
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field__' + filter_type: None } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = 'should fail'
            with self.assertRaises( InvalidFilterError ):
                q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )

        for filter_type in QUERY_LIST_OPERATORS:

            value = str( object_id )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field__' + filter_type: [ object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field__' + filter_type: [ object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = 'should fail'
            with self.assertRaises( InvalidFilterError ):
                q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )

            id_list = [ ObjectId(), ObjectId() ]
            stringed_id_list = [ str( value ) for value in id_list ]
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: stringed_id_list }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field__' + filter_type: id_list } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_relational_look_up_filter( self ):

        for field in self.data.document_fields:

            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + field + '__exists': 'True' }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field__in': [ str( self.data.document.id ) ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__to_one_field__' + field + '__exists': 'True' }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field__in': [ str( self.data.document.id ) ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_filtering_on_unregistered_related_fields( self ):
        """
        We allow filtering on related fields of the document even though they are not registered on the resource. As
        the resource might not know what kind of fields these are, this is a special case for filtering which used to
        fail.
        """

        object_id = ObjectId()
        value = str( object_id )

        for filter_type in QUERY_EQUALITY_OPERATORS:
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field_not_on_resource__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field_not_on_resource__' + filter_type: object_id } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )
        for filter_type in  QUERY_LIST_OPERATORS:
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field_not_on_resource__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_one_field_not_on_resource__' + filter_type: [ object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

        for filter_type in { 'exact', 'ne' } | QUERY_LIST_OPERATORS:
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field_not_on_resource__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_many_field_not_on_resource__' + filter_type: [ object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_in_empty_list( self ):
        """
        The wanted behavior of a filter like id__in=[], is to return zero. Mongo handles this well, do we?
        """

        for filter_type in QUERY_LIST_OPERATORS:

            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: '' }, None )
            self.assertDictEqual( q_filter.query, { 'id_field__' + filter_type: [] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

    def test_list_dict_doc_fields_filtering( self ):
        """
        ListFields, DictFields and EmbeddedDocumentFields can't be filtered on through the api, because their
        structures are too complex. Test to see whether filtering gets rejected for filter types other than size and
        exists.
        """

        for field in ( 'list_field', 'dict_field', 'document_field' ):

            for filter_type in QUERY_EQUALITY_OPERATORS | QUERY_LIST_OPERATORS:
                with self.assertRaises( InvalidFilterError ):
                    q_filter = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: 'should fail' }, None )

    def test_to_many_field_filter( self ):
        object_id = ObjectId()

        for filter_type in ( 'exact', 'ne' ):

            value = str( object_id )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_many_field__' + filter_type: [ object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_many_field__' + filter_type: [ object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = [ str( object_id ), str( object_id ) ]
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_many_field__' + filter_type: [ object_id, object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = [ 'null' ]
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_many_field__' + filter_type: [] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = 'should fail'
            with self.assertRaises( InvalidFilterError ):
                q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field__' + filter_type: value }, None )

        for filter_type in QUERY_LIST_OPERATORS:

            value = str( object_id )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_many_field__' + filter_type: [ object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_many_field__' + filter_type: [ object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = [ str( object_id ), str( object_id ) ]
            q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field__' + filter_type: value }, None )
            self.assertDictEqual( q_filter.query, { 'to_many_field__' + filter_type: [ object_id, object_id ] } )
            result = list( self.data.resource.get_queryset( self.data.request ).filter( q_filter ) )

            value = 'should fail'
            with self.assertRaises( InvalidFilterError ):
                q_filter = self.data.allfieldsdocument_resource.build_filters( { 'to_many_field__' + filter_type: value }, None )
