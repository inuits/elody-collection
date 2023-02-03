import csv
import io


def map_metadata_to_csv(metadata):
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
