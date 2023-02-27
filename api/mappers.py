import csv
import io


def can_append_key(key, fields):
    if not fields:
        return True
    return key in fields


def csv_writter(header, rows):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def map_data_according_to_accept_header(
    data, accept_header, data_type="metadata", fields=None
):
    match accept_header:
        case "text/csv":
            return map_to_csv(data, data_type, fields)
        case _:
            return data


def map_entities_to_csv(entities, fields=None):
    keys = list()
    root_values = list()
    for entity in entities:
        values = list()
        for id in entity.get("identifiers", []):
            if not can_append_key("identifiers", fields):
                values.append({})
                break
            if "identifier" not in keys:
                keys.append("identifier")
            values.append({0: id})
        if can_append_key("type", fields):
            if "type" not in keys:
                keys.append("type")
            values[0][1] = entity.get("type")
        for metadata in entity.get("metadata", []):
            key = metadata.get("key")
            if not can_append_key(key, fields):
                continue
            if key not in keys:
                keys.append(metadata.get("key"))
            values[0][keys.index(key)] = metadata.get("value")
        for i in range(len(keys)):
            if i not in values[0]:
                values[0][i] = None
        values[0] = dict(sorted(values[0].items()))
        root_values += [list(row.values()) for row in values]
    return csv_writter(keys, root_values)


def map_entity_to_csv(entity, fields=None):
    keys = list()
    values = list()
    for id in entity.get("identifiers", []):
        if not can_append_key("identifiers", fields):
            values.append([])
            break
        if "identifier" not in keys:
            keys.append("identifier")
        values.append([id])
    if can_append_key("type", fields):
        keys.append("type")
        values[0].append(entity.get("type"))
    for metadata in entity.get("metadata", []):
        key = metadata.get("key")
        if not can_append_key(key, fields):
            continue
        keys.append(key)
        values[0].append(metadata.get("value"))
    for relation in entity.get("relations", []):
        label = relation.get("label")
        if not can_append_key(label, fields):
            continue
        keys.append(label)
        values[0].append(relation.get("key"))
    return csv_writter(keys, values)


def map_metadata_to_csv(metadata, fields=None):
    keys = list()
    values = list()
    for field in metadata:
        key = field.get("key")
        if not can_append_key(key, fields):
            continue
        keys.append(key)
        values.append(field.get("value"))
    return csv_writter(keys, [values])


def map_to_csv(data, data_type, fields=None):
    match data_type:
        case "metadata":
            return map_metadata_to_csv(data, fields)
        case "entities":
            return map_entities_to_csv(data["results"], fields)
        case "entity":
            return map_entity_to_csv(data, fields)
        case _:
            return data
