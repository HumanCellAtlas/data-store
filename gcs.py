#!/usr/bin/env python

"""
DSS description FIXME: elaborate
"""

import os, sys, json, time
from datetime import datetime, timedelta

import boto3
import google.cloud.storage
from azure.storage.blob import BlockBlobService, BlobPermissions
from flask import Flask, request, redirect, jsonify
from flask_restplus import Resource, Api, reqparse, fields

app = Flask(__name__)
api = Api(app, validate=True)

file_ns = api.namespace("files", description="File objects")

@file_ns.route('/')
class FileCollection(Resource):
    def get(self):
        return {}

dss_file = api.model('File', {
    'id': fields.String(required=True, location='values'),
    'name': fields.String(required=True, description='File name'),
})

@file_ns.route('/<string:file_id>')
class FileItem(Resource):
    @api.response(302, 'Redirect to file location.')
    def get(self, file_id):
        return {}

    @api.expect(dss_file)
    def post(self, file_id):
        return api.payload

dss_bundle = api.model('Bundle', {
    'id': fields.String(required=True),
    'contents': fields.List(fields.String, description='List of files in the bundle'),
    'metadata': fields.String(required=True, description='Bundle metadata')
})

@app.route("/swagger")
def swagger():
    return jsonify(api.__schema__)

if __name__ == '__main__':
    app.run(debug=True)
