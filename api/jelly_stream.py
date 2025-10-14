from typing import Union, List, Tuple
from rdflib import Graph, URIRef, Literal, Namespace, RDF
from pyjelly.integrations.rdflib.serialize import SerializerOptions, StreamParameters
from urllib.parse import quote
from io import BytesIO

EX = Namespace("http://example.org/")
Triple = Tuple[URIRef, URIRef, Union[URIRef, Literal]]
options = SerializerOptions(params=StreamParameters(namespace_declarations=True))


def mongo_to_triples(doc) -> List[Triple]:
    triples: List[Triple] = []
    subject = URIRef(f"http://example.org/{quote(str(doc['_id']))}") # quote to ensure valid URI

    # type
    triples.append((subject, RDF.type, EX[doc.get("type", "Unknown").capitalize()]))

    # metadata
    for meta in doc.get("metadata", []):
        triples.append((subject, EX[meta["key"]], Literal(meta["value"])))

    # identifiers
    for ident in doc.get("identifiers", []):
        triples.append((subject, EX.identifier, Literal(ident)))

    # relations
    for rel in doc.get("relations", []):
        triples.append((subject, EX[rel["type"]], EX[rel["key"]]))
        if "roles" in rel:
            for role in rel["roles"]:
                triples.append((subject, EX.role, Literal(role)))

    # location
    if "location" in doc and "coordinates" in doc["location"]:
        lon, lat = doc["location"]["coordinates"]
        triples.append((subject, EX.lat, Literal(lat)))
        triples.append((subject, EX.lon, Literal(lon)))

    return triples


def generate_jelly_stream(db, collection_name, batch_size):
    entities = db[collection_name].find({'type': 'sensorDetection'})

    batch = []
    for entity in entities:
        batch.append(entity)

        if len(batch) >= batch_size:
            yield serialize_batch_to_jelly(batch)
            batch = []

    if batch:
        yield serialize_batch_to_jelly(batch)



def serialize_batch_to_jelly(batch):
    graph = Graph()
    graph.namespace_manager.bind("ex", EX)

    for entity in batch:
        for subject, predicate, obj in mongo_to_triples(entity):
            graph.add((subject, predicate, obj))

    buffer = BytesIO()
    graph.serialize(destination=buffer, format="jelly", options=options)
    return buffer.getvalue()