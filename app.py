from flask import Flask
from flask_restful import Api
from flask_restful_swagger import swagger
from resources.tenant import Tenant, TenantDetail
from resources.asset import Asset, AssetDetail


app = Flask(__name__)

api = swagger.docs(Api(app), apiVersion='0.1')

api.add_resource(TenantDetail, '/tenants/<string:id>')
api.add_resource(Tenant, '/tenants')

api.add_resource(AssetDetail, '/assets/<string:id>')
api.add_resource(Asset, '/assets')

if __name__ == '__main__':
    app.run(debug=True)
