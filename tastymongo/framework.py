from pyramid.response import Response

class HttpResponse( Response ):
    status_int = 200

class HttpCreated( Response ):
    status_int = 201

class HttpMethodNotAllowed( Response ):
    status_int = 405