import csv
import io


def csv_writter(header, rows):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def map_data_according_to_accept_header(data, accept_header, data_type="metadata"):
    match accept_header:
        case "text/csv":
            return map_to_csv(data, data_type)
        case _:
            return data


def map_entities_to_csv(entities):
    keys = list()
    root_values = list()
    key_positions = dict()
    for entity in entities:
        values = list()
        for id in entity.get("identifiers", []):
            if "identifier" not in keys:
                keys.append("identifier")
            values.append({0: id})
        if "type" not in keys:
            keys.append("type")
        values[0][1] = entity.get("type")
        for metadata in entity.get("metadata", []):
            key = metadata.get("key")
            if key not in keys:
                keys.append(metadata.get("key"))
            values[0][keys.index(key)] = metadata.get("value")
        for i in range(len(keys)):
            if i not in values[0]:
                values[0][i] = None
        values[0] = dict(sorted(values[0].items()))
        root_values += [list(row.values()) for row in values]
    return csv_writter(keys, root_values)


def map_entity_to_csv(entity):
    keys = list()
    values = list()
    for id in entity.get("identifiers", []):
        if "identifier" not in keys:
            keys.append("identifier")
        values.append([id])
    keys.append("type")
    values[0].append(entity.get("type"))
    for metadata in entity.get("metadata", []):
        keys.append(metadata.get("key"))
        values[0].append(metadata.get("value"))
    for relation in entity.get("relations", []):
        keys.append(relation.get("label"))
        values[0].append(relation.get("key"))
    return csv_writter(keys, values)


def map_metadata_to_csv(metadata):
    keys = list()
    values = list()
    for field in metadata:
        keys.append(field.get("key"))
        values.append(field.get("value"))
    return csv_writter(keys, [values])


def map_to_csv(data, data_type):
    match data_type:
        case "metadata":
            return map_metadata_to_csv(data)
        case "entities":
            return map_entities_to_csv(data["results"])
        case "entity":
            return map_entity_to_csv(data)
        case _:
            return data
