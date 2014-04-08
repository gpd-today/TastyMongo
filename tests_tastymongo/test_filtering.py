from __future__ import print_function
from __future__ import unicode_literals

import unittest

import mongoengine

from pyramid import testing

from tastymongo.api import Api
from tastymongo.constants import *
import tastymongo.fields as fields
from tastymongo.exceptions import InvalidFilterError

from bson import ObjectId
from bson.errors import InvalidId
from tests_tastymongo.utils import Struct
from tests_tastymongo.documents import AllFieldsDocument
from tests_tastymongo.resources import AllFieldsDocumentResource
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
            document_field = None,
            date_field = datetime.date.today(),
            datetime_field = datetime.datetime.today(),
            time_field = datetime.datetime.today().time(),
            to_one_field = None,
            to_many_field = None
        )
        self.data.document.save()

        self.data.api_url = self.data.allfieldsdocument_resource._meta.api.route


    def tearDown( self ):
        testing.tearDown()

        # Clear data
        self.data = None

    # def test_document_to_uri( self ):
    #     d = self.data
    #     d.resource.build_filters( {}, None )
    #     filter_values_per_field_type = {
    #         'id_field': ObjectId(),
    #         'string_field': 'hello world',
    #         'int_field': 1,
    #         'float_field': 2.3,
    #         'decimal_field': Decimal( 1 / 3 ),
    #         'boolean_field': True,
    #         'list_field': [ 'hi', 'world' ],
    #         'dict_field': { 'hello': 'earth' },
    #         'document_field': None,
    #         'date_field': fields.DateField(),
    #         'datetime_field': fields.DateTimeField(),
    #         'time_field': fields.TimeField()
    #     }
    #
    #     for filter_type in QUERY_EQUALITY_OPERATORS:
    #         for field_type in field_types:
    #             a=1

    def test_match_operator_filter( self ):

        unallowed_fields = ( 'id_field', 'int_field', 'float_field', 'decimal_field', 'boolean_field', 'list_field',
            'dict_field', 'document_field', 'date_field', 'datetime_field', 'time_field' )

        for filter_type in QUERY_MATCH_OPERATORS:

            for field in unallowed_fields:
                with self.assertRaises( InvalidFilterError ):
                    query = self.data.allfieldsdocument_resource.build_filters( { field + '__' + filter_type: None }, None )


    def test_objectid_field_filter( self ):

        for filter_type in QUERY_EQUALITY_OPERATORS:

            object_id = ObjectId()

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
            with self.assertRaises( InvalidId ):
                query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )

        for filter_type in QUERY_LIST_OPERATORS:

            value = str( object_id )
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: [ object_id ] } )

            value = '{0}/allfieldsdocument/{1}/'.format( self.data.api_url, str( object_id ) )
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: [ object_id ] } )

            value = 'null'
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: [ None ] } )

            value = 'should fail'
            with self.assertRaises( InvalidId ):
                query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )

            id_list = [ ObjectId(), ObjectId() ]
            stringed_id_list = [ str( value ) for value in id_list ]
            query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: stringed_id_list }, None )
            self.assertDictEqual( query.query, { 'id_field__' + filter_type: id_list } )


    # def test_string_field_filter( self ):
    #
    #     for filter_type in QUERY_EQUALITY_OPERATORS | QUERY_MATCH_OPERATORS:
    #
    #         object_id = str( ObjectId() )
    #
    #         value = object_id
    #         query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
    #         self.assertEqual( query.query, { 'id_field__' + filter_type: object_id } )
    #
    #         value = '/api/v1/test/{0}/'.format( str( object_id ) )
    #         query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
    #         self.assertEqual( query.query, { 'id_field__' + filter_type: object_id } )
    #
    #         value = 'null'
    #         query = self.data.allfieldsdocument_resource.build_filters( { 'id_field__' + filter_type: value }, None )
    #         self.assertEqual( query.query, { 'id_field__' + filter_type: None } )