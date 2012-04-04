from pyramid.response import Response

class HttpResponse(Response):
    def __init__(self, *args, **kwargs):
        slef.status_int = 200


class HttpCreated(Response):
    def __init__(self, *args, **kwargs):
        location = ''

        if 'location' in kwargs:
            location = kwargs['location']
            del( kwargs['location'] )

        super(HttpCreated, self).__init__(*args, **kwargs)
        self.location = location
        self.status_int = 201


class HttpAccepted(Response):
    def __init__(self, *args, **kwargs):
        super(HttpAccepted, self).__init__(*args, **kwargs)
        self.status_int = 202


class HttpNoContent(Response):
    def __init__(self, *args, **kwargs):
        super(HttpNoContent, self).__init__(*args, **kwargs)
        self.status_int = 204


class HttpNotModified(Response):
    def __init__(self, *args, **kwargs):
        super(HttpNotModified, self).__init__(*args, **kwargs)
        self.status_int = 304


class HttpBadRequest(Response):
    def __init__(self, *args, **kwargs):
        super(HttpBadRequest, self).__init__(*args, **kwargs)
        self.status_int = 400


class HttpUnauthorized(Response):
    def __init__(self, *args, **kwargs):
        super(HttpUnauthorized, self).__init__(*args, **kwargs)
        self.status_int = 401


class HttpForbidden(Response):
    def __init__(self, *args, **kwargs):
        super(HttpForbidden, self).__init__(*args, **kwargs)
        self.status_int = 403


class HttpNotFound(Response):
    def __init__(self, *args, **kwargs):
        super(HttpNotFound, self).__init__(*args, **kwargs)
        self.status_int = 404

class HttpMethodNotAllowed(Response):
    def __init__(self, *args, **kwargs):
        super(HttpMethodNotAllowed, self).__init__(*args, **kwargs)
        self.status_int = 405


class HttpGone(Response):
    def __init__(self, *args, **kwargs):
        super(HttpGone, self).__init__(*args, **kwargs)
        self.status_int = 410


class HttpApplicationError(Response):
    def __init__(self, *args, **kwargs):
        super(HttpApplicationError, self).__init__(*args, **kwargs)
        self.status_int = 500


class HttpNotImplemented(Response):
    def __init__(self, *args, **kwargs):
        super(HttpNotImplemented, self).__init__(*args, **kwargs)
        self.status_int = 501