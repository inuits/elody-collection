from flask_restful import Resource, reqparse, abort

TENANTS = {}

def abort_if_asset_doesnt_exist(id):
    if id not in TENANTS:
        abort(404, message="Asset {} doesn't exist".format(id))

class Asset(Resource):
    parser = reqparse.RequestParser()  # only allow price changes, no name changes allowed
    parser.add_argument('title')
    parser.add_argument('id')

    def post(self):
        args = Asset.parser.parse_args()
        asset = {'title': args['title']}
        TENANTS[args['id']] = asset
        return asset, 201

class AssetDetail(Resource):
    def get(self, id):
        abort_if_asset_doesnt_exist(id)
        return TENANTS[id]
