from pyramid.response import Response

class HttpResponse(Response):
    status_int = 200


class HttpCreated(Response):
    status_int = 201

    def __init__(self, *args, **kwargs):
        location = ''

        if 'location' in kwargs:
            location = kwargs['location']
            del( kwargs['location'] )

        super(HttpCreated, self).__init__(*args, **kwargs)
        self.location = location


class HttpCreated(Response):
    status_int = 201


class HttpAccepted(Response):
    status_int = 202


class HttpNoContent(Response):
    status_int = 204


class HttpNotModified(Response):
    status_int = 304


class HttpBadRequest(Response):
    status_int = 400


class HttpUnauthorized(Response):
    status_int = 401


class HttpForbidden(Response):
    status_int = 403


class HttpMethodNotAllowed(Response):
    status_int = 405


class HttpGone(Response):
    status_int = 410


class HttpApplicationError(Response):
    status_int = 500


class HttpNotImplemented(Response):
    status_int = 501