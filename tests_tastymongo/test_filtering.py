from __future__ import print_function
from __future__ import unicode_literals

import unittest

from pyramid import testing

from tastymongo.constants import *
from tastymongo.exceptions import InvalidFilterError

from bson import ObjectId
from tests_tastymongo.documents import AllFieldsDocument, EmbeddedDoc
from tests_tastymongo.run_tests import setup_db, setup_request

from decimal import Decimal
import datetime

class BasicTests( unittest.TestCase ):

    def setUp( self ):
        self.conn = setup_db()
        self.data = setup_request()

        # Setup data
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
        self.data.document.save()
        self.data.document.to_one_field = self.data.document
        self.data.document.to_many_field = [ self.data.document ]
        self.data.document.save()

        self.data.api_url = self.data.allfieldsdocument_resource._meta.api.route

        self.data.document_fields = { 'id_field', 'string_field', 'int_field', 'float_field', 'decimal_field',
        'boolean_field', 'list_field', 'dict_field', 'document_field', 'date_field', 'datetime_field', 'time_field',
        'to_one_field', 'to_many_field' }


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
                    query = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: None }, None )

            # and that we get a decent query for string fields:
            query = self.data.allfieldsdocument_resource.build_filters( { 'string_field' + '__' + filter_type: 'test' }, None )
            self.assertDictEqual( query.query, { 'string_field__' + filter_type: 'test' } )


    def test_objectid_field_filter( self ):
        object_id = ObjectId()

        for filter_type in QUERY_EQUALITY_OPERATORS:

            value = str( object_id )
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: object_id } )

            value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: object_id } )

            value = 'null'
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: None } )

            value = 'should fail'
            with self.assertRaises( InvalidFilterError ):
                query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )

        for filter_type in QUERY_LIST_OPERATORS:

            value = str( object_id )
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: [ object_id ] } )

            value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: [ object_id ] } )

            value = 'should fail'
            with self.assertRaises( InvalidFilterError ):
                query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )

            id_list = [ ObjectId(), ObjectId() ]
            stringed_id_list = [ str( value ) for value in id_list ]
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: stringed_id_list }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: id_list } )


    def test_string_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            # we pick the string 'None' which should not be recognized as anything but a string
            value = 'None'

            query = self.data.allfieldsdocument_resource.build_filters( { 'string_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'string_field__' + filter_type: value } )

        for filter_type in QUERY_LIST_OPERATORS:

            value = 'None'

            query = self.data.allfieldsdocument_resource.build_filters( { 'string_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'string_field__' + filter_type: [ value ] } )

    def test_int_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            for value in ( -1.9, -1.2, -1, 0, 0.0, 3, 3.2, 3.9, 3.4999999999999999999999999999999999999999999 ):
                query = self.data.allfieldsdocument_resource.build_filters( { 'int_field__' + filter_type: str( value ) }, None )
                self.assertDictEqual( query.query, { 'int_field__' + filter_type: round( value ) } )

        for filter_type in QUERY_LIST_OPERATORS:

            value = 3
            query = self.data.allfieldsdocument_resource.build_filters( { 'int_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( query.query, { 'int_field__' + filter_type: [ value ] } )

    def test_float_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            for value in ( -1.9, -1.2, -1, 0, 0.0, 3, 3.2, 3.9, 3.4999999999999999999999999999999999999999999 ):
                query = self.data.allfieldsdocument_resource.build_filters( { 'float_field__' + filter_type: str( value ) }, None )
                self.assertDictEqual( query.query, { 'float_field__' + filter_type: value } )

        for filter_type in QUERY_LIST_OPERATORS:

            value = 3.9
            query = self.data.allfieldsdocument_resource.build_filters( { 'float_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( query.query, { 'float_field__' + filter_type: [ value ] } )

    def test_decimal_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            value = Decimal( 4.8 )
            query = self.data.allfieldsdocument_resource.build_filters( { 'decimal_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( query.query, { 'decimal_field__' + filter_type: value } )

        for filter_type in QUERY_LIST_OPERATORS:

            value = Decimal( 4.9 )
            query = self.data.allfieldsdocument_resource.build_filters( { 'decimal_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( query.query, { 'decimal_field__' + filter_type: [ value ] } )

    def test_boolean_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            value = True
            query = self.data.allfieldsdocument_resource.build_filters( { 'boolean_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( query.query, { 'boolean_field__' + filter_type: value } )

            value = False
            query = self.data.allfieldsdocument_resource.build_filters( { 'boolean_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( query.query, { 'boolean_field__' + filter_type: value } )

            query = self.data.allfieldsdocument_resource.build_filters( { 'boolean_field__' + filter_type: 'null' }, None )
            self.assertDictEqual( query.query, { 'boolean_field__' + filter_type: None } )

        for filter_type in QUERY_LIST_OPERATORS:

            value = True
            query = self.data.allfieldsdocument_resource.build_filters( { 'boolean_field__' + filter_type: str( value ) }, None )
            self.assertDictEqual( query.query, { 'boolean_field__' + filter_type: [ value ] } )

    def test_to_one_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            object_id = ObjectId()

            value = str( object_id )
            query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'to_one_field__' + filter_type: object_id } )

            value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
            query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'to_one_field__' + filter_type: object_id } )

            value = 'null'
            query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'to_one_field__' + filter_type: None } )

            value = 'should fail'
            with self.assertRaises( InvalidFilterError ):
                query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )

        for filter_type in QUERY_LIST_OPERATORS:

            value = str( object_id )
            query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'to_one_field__' + filter_type: [ object_id ] } )

            value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
            query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'to_one_field__' + filter_type: [ object_id ] } )

            value = 'should fail'
            with self.assertRaises( InvalidFilterError ):
                query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: value }, None )

            id_list = [ ObjectId(), ObjectId() ]
            stringed_id_list = [ str( value ) for value in id_list ]
            query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + filter_type: stringed_id_list }, None )
            self.assertDictEqual( query.query, { 'to_one_field__' + filter_type: id_list } )

    def test_relational_look_up_filter( self ):

        for field in self.data.document_fields:

            query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__' + field + '__exists': 'True' }, None )
            self.assertDictEqual( query.query, { 'to_one_field__in': [ str( self.data.document.id ) ] } )

            query = self.data.allfieldsdocument_resource.build_filters( { 'to_one_field__to_one_field__' + field + '__exists': 'True' }, None )
            self.assertDictEqual( query.query, { 'to_one_field__in': [ str( self.data.document.id ) ] } )

    def test_in_empty_list( self ):
        """
        The wanted behavior of a filter like id__in=[], is to return zero. Mongo handles this well, do we?
        """

        for filter_type in QUERY_LIST_OPERATORS:

            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: '' }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: [] } )

