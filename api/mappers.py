import csv
import io


def map_to_csv(metadata):
    output = io.StringIO()
    writer = csv.writer(output)
    keys = list()
    values = list()
    for field in metadata:
        keys.append(field.get("key"))
        values.append(field.get("value"))
    writer.writerow(keys)
    writer.writerow(values)
    return output.getvalue()


def map_data_according_to_accept_header(data, accept_header):
    match accept_header:
        case "text/csv":
            return map_to_csv(data)
        case _:
            return data
