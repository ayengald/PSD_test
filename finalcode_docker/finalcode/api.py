from flask import Flask, request
from flask_restful import Resource, Api
import psd

app = Flask(__name__)
api = Api(app)

class psd_runner(Resource):
        def get(self):
                psd.psd()
api.add_resource(psd_runner, '/psd')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)