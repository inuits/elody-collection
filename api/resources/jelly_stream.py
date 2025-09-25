from flask import request, Response
from flask_restful import Resource
from jelly_stream import generate_jelly_stream
from resources.base_resource import BaseResource



class JellyStreamingResource(BaseResource):
    def get(self):
        db_name = request.args.get('db', 'hairoad')
        collection_name = request.args.get('collection', 'entities')
        batch_size = int(request.args.get('batch_size', 100))

        if not 1 <= batch_size <= 1000:
            return {"error": "batch_size must be between 1 and 1000"}, 400

        def generate():
                for chunk in generate_jelly_stream(self.storage, collection_name, batch_size):
                    yield chunk
                return []

        return Response(
            list(generate()), 
            mimetype='application/octet-stream',
            headers={
                'Content-Disposition': f'attachment; filename={collection_name}.jelly',
                'X-Content-Format': 'jelly-rdf',
                'Cache-Control': 'no-cache'
            }
        )