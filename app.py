import tornado.ioloop
import tornado.web
import os
import re
import uuid

from time import gmtime, strftime


content_regex = '^application/vnd\.redhat\.([a-z]+)\.([a-z]+)\+(tgz|zip)$'
# set max length to 10.5 MB (one MB larger than peak)
max_length = os.getenv('MAX_LENGTH', 11010048)
listen_port = os.getenv('LISTEN_PORT', 8888)

# TODO: replace this with the MQ or some other key/value store
file_dict = {}

# TODO: create an actual persistent store for id status
status = {}

# all storage below is local for testing. need to decide on a real object store
# - s3 for permanent, PVC for quarantine zone?
# TODO: decide on storage locations for quarantine and permanent


def upload_validation(upload):
    if int(upload['Content-Length']) >= max_length:
        error = (413, 'Payload too large: ' + upload['Content-Length'])
        return error
    if re.search(content_regex, upload['Content-type']) is None:
        error = (415, 'Unsupported Media Type')
        return error


def split_content(content):
    service = content.split('.')[2]
    filename = content.split('.')[-1]
    return service, filename


def service_notify(payload):
    # Report the new upload to the proper message queue to be ingested by
    # targeted service
    return 'boop'


class RootHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("boop")

    def options(self):
        self.add_header('Allow', 'GET, HEAD, OPTIONS')


class UploadHandler(tornado.web.RequestHandler):
    # accepts uploads. No auth implemented yet, likely to be handled by 3scale
    # anyway.

    def get(self):
        self.write("Accepted Content-Types: gzipped tarfile, zip file")

    def post(self):
        invalid = upload_validation(self.request.headers)
        if invalid:
            self.set_status(invalid[0], invalid[1])
        else:
            service, filename = split_content(self.request.headers['Content-type'])
            hash_value = uuid.uuid4().hex
            try:
                with open('/datastore/' + service + '/' + hash_value, 'w') as f:
                    f.write(self.request.body)
                    file_dict[hash_value] = '/datastore/' + service + '/' + hash_value
            except IOError:
                print('Service name does not exist: ' + service)
                self.set_status(415, 'Unknown Service')
                self.finish()
            status[hash_value] = {'upload_status': 'recieved',
                                  'update_time': strftime("%Y%m%d-%H:%M:%S", gmtime())}
            self.add_header('Status-Endpoint', '/api/v1/upload/status?id=' + hash_value)
            self.set_status(202, 'Accepted')
            # once MQ is decided on, the service_notify function will go here

    def options(self):
        self.add_header('Allow', 'GET, POST, HEAD, OPTIONS')


class UploadStatus(tornado.web.RequestHandler):
    # endpoint for getting information on an individual upload.
    # Was upload rejected or eccepted by service

    def get(self):
        upload_id = self.get_argument('id')
        if upload_id == []:
            self.set_status(400)
            return self.finish("Invalid ID")
        self.write(status[upload_id])


class TmpFileHandler(tornado.web.RequestHandler):
    # temporary location for apps to grab the files from. once validated,
    # they send an empty PUT request to approve it or a DELETE request to
    # remove it. If approved, it moves to a more permanent location with a
    # new URI

    def __init__(self):

        self.hash_value = self.request.uri.split('/')[4]

    def get(self):
        buf_size = 4096
        with open(file_dict[self.hash_value], 'r') as f:
            while True:
                data = f.read(buf_size)
                if not data:
                    self.set_status(404)
                    break
                status[self.hash_value]['upload_status'] = 'validating'
                status[self.hash_value]['update_time'] = strftime("%Y%m%d-%H:%M:%S", gmtime())
                self.set_status(200, 'OK')
                self.write(data)
        self.finish()

    def put(self):
        new_path = "/tmp/new_dir/" + file_dict[self.hash_value].split('/')[-1]
        os.rename(file_dict[self.hash_value], new_path)
        file_dict[self.hash_value] = new_path
        status[self.hash_value]['upload_status'] = 'accepted'
        status[self.hash_value]['update_time'] = strftime("%Y%m%d-%H:%M:%S", gmtime())
        self.set_status(204, 'No Content')
        self.add_header('Package-URI', "/v1/store/" + self.hash_value)

    def delete(self):
        status[self.hash_value]['upload_status'] = 'rejected'
        status[self.hash_value]['update_time'] = strftime("%Y%m%d-%H:%M:%S", gmtime())
        self.set_status(202, 'Accepted')
        os.remove(file_dict[self.hash_value])
        file_dict.pop(self.hash_value, none)


class StaticFileHandler(tornado.web.RequestHandler):
    # Location for grabbing file from the long term storage

    def get(self):
        # use the storage broker service to grab the file from S3
        self.write('booop placeholder')


class VersionHandler(tornado.web.RequestHandler):

    def get(self):
        response = {'version': '0.0.1'}
        self.write(response)


endpoints = [
    (r"/", RootHandler),
    (r"/api/v1/version", VersionHandler),
    (r"/api/v1/upload", UploadHandler),
    (r"/api/v1/upload/status", UploadStatus),
    (r"/api/v1/tmpstore/\w{32}", TmpFileHandler),
    (r"/api/v1/store/\w{32}", StaticFileHandler)
]

app = tornado.web.Application(endpoints)


if __name__ == "__main__":
    app.listen(listen_port)
    tornado.ioloop.IOLoop.current().start()
