from pyramid.response import Response

class HTTPResponse(Response):
    def __init__(self, *args, **kwargs):
        super(HTTPResponse, self).__init__( *args, **kwargs )
        self.status_int = 200


class HTTPCreated(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPCreated, self).__init__(*args, **kwargs)
        self.location = kwargs.pop('location', '')
        self.status_int = 201


class HTTPAccepted(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPAccepted, self).__init__(*args, **kwargs)
        self.status_int = 202


class HTTPNoContent(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPNoContent, self).__init__(*args, **kwargs)
        self.status_int = 204


class HTTPMultipleChoices(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPMultipleChoices, self).__init__(*args, **kwargs)
        self.status_int = 300 


class HTTPNotModified(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPNotModified, self).__init__(*args, **kwargs)
        self.status_int = 304


class HTTPBadRequest(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPBadRequest, self).__init__(*args, **kwargs)
        self.status_int = 400


class HTTPUnauthorized(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPUnauthorized, self).__init__(*args, **kwargs)
        self.status_int = 401


class HTTPForbidden(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPForbidden, self).__init__(*args, **kwargs)
        self.status_int = 403


class HTTPNotFound(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPNotFound, self).__init__(*args, **kwargs)
        self.status_int = 404


class HTTPMethodNotAllowed(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPMethodNotAllowed, self).__init__(*args, **kwargs)
        self.status_int = 405


class HTTPGone(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPGone, self).__init__(*args, **kwargs)
        self.status_int = 410


class HTTPInternalServerError(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPInternalServerError, self).__init__(*args, **kwargs)
        self.status_int = 500


class HTTPNotImplemented(HTTPResponse):
    def __init__(self, *args, **kwargs):
        super(HTTPNotImplemented, self).__init__(*args, **kwargs)
        self.status_int = 501
