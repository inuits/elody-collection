import glob
import json
import os
import pandas as pd
import requests


class Importer:
    def __init__(self, collection_api_url, storage_api_url):
        self.collection_api_url = collection_api_url
        self.storage_api_url = storage_api_url
        self.upload_location = requests.get(
            self.collection_api_url + "/importer/location",
            headers={"Content-Type": "application/json"},
        ).json()

    def metadata_up_to_date(self, new_metadata, all_metadata):
        return all(elem in all_metadata for elem in new_metadata)

    def get_new_metadata(self, rights, copyright):
        new_metadata = []
        if not pd.isna(rights):
            new_metadata.append({"key": "rights", "value": rights})
        if not pd.isna(copyright):
            new_metadata.append({"key": "copyright", "value": copyright})
        return new_metadata

    def no_metadata_available(self, rights, copyright):
        return pd.isna(rights) and pd.isna(copyright)

    def add_metadata_to_entity(self, object_id, rights, copyright):
        if self.no_metadata_available(rights, copyright):
            return None
        new_metadata = self.get_new_metadata(rights, copyright)
        all_metadata = requests.get(
            "{}/entities/{}/metadata".format(self.collection_api_url, object_id),
            headers={"Content-Type": "application/json"},
        ).json()
        if not all_metadata:
            all_metadata = new_metadata
        else:
            if self.metadata_up_to_date(new_metadata, all_metadata):
                return all_metadata
            for index, data in enumerate(all_metadata):
                if ("key", "copyright") in data.items() and not pd.isna(copyright):
                    all_metadata[index] = new_metadata.pop()
                if ("key", "rights") in data.items() and not pd.isna(rights):
                    all_metadata[index] = new_metadata.pop()
            all_metadata = all_metadata + new_metadata
        return requests.patch(
            "{}/entities/{}/metadata".format(self.collection_api_url, object_id),
            headers={"Content-Type": "application/json"},
            data=json.dumps(all_metadata),
        ).json()

    def create_mediafile(self, object_id, file_name):
        # Fix to include file or whatever this request needs
        content = {
            "type": "mediafile",
            "location": "{}/download/{}".format(self.storage_api_url, file_name),
        }
        # This could be simplified by using the /entities/id/mediafiles/create endpoint
        # The endpoint needs some tweaking first
        mediafile = requests.post(
            "{}/mediafiles".format(self.collection_api_url),
            headers={"Content-Type": "application/json"},
            data=json.dumps(content),
        ).json()
        ret = requests.post(
            "{}/entities/{}/mediafiles".format(self.collection_api_url, object_id),
            headers={"Content-Type": "application/json"},
            data=json.dumps(mediafile),
        )
        if ret.status_code != 201:
            content = {"identifiers": [object_id], "type": "asset"}
            requests.post(
                "{}/entities".format(self.collection_api_url),
                headers={"Content-Type": "application/json"},
                data=json.dumps(content),
            )
            ret = requests.post(
                "{}/entities/{}/mediafiles".format(self.collection_api_url, object_id),
                headers={"Content-Type": "application/json"},
                data=json.dumps(mediafile),
            ).json()
        return ret

    def upload_file(self, file_name, file_path):
        upload_location = "{}/upload/{}".format(self.storage_api_url, file_name)
        files = {"file": open(file_path, "rb")}
        requests.post(upload_location, files=files)

    def write_to_db(self, object_id, file_name, file_path, row):
        self.upload_file(file_name, file_path)
        mediafile = self.create_mediafile(object_id, file_name)
        metadata = self.add_metadata_to_entity(
            object_id, row["Rechtenstatus"], row["Copyright"]
        )

    def parse_path(self, path, row):
        if pd.isna(row["Padnaam"]):
            file_path = sorted(
                glob.glob(path + "**/" + row["Bestandsnaam"], recursive=True)
            )[0]
        else:
            if row["Padnaam"][1] == ":":
                file_path = str.replace(row["Padnaam"][3:], "\\", "/")
            else:
                file_path = (
                    row["Padnaam"][1:] if row["Padnaam"][0] == "/" else row["Padnaam"]
                )
            file_path = os.path.join(self.upload_location, file_path)
        return file_path

    def is_malformed_row(self, row):
        return pd.isna(row["Objectnummer"]) or (
            pd.isna(row["Padnaam"]) and pd.isna(row["Bestandsnaam"])
        )

    def parse_rows(self, path, combined_csv):
        for index, row in combined_csv.iterrows():
            if self.is_malformed_row(row):
                continue
            file_path = self.parse_path(path, row)
            file_name = os.path.basename(file_path)
            object_id = row["Objectnummer"]
            self.write_to_db(object_id, file_name, file_path, row)

    def validate_csv(self, df):
        return "Objectnummer" in df.columns and (
            "Bestandsnaam" in df.columns or "Padnaam" in df.columns
        )

    def read_csv(self, all_csv_files):
        dataframes = []
        for f in all_csv_files:
            df = pd.read_csv(f)
            df.columns = df.columns.str.capitalize()
            if not self.validate_csv(df):
                continue
            dataframes.append(df)
        return dataframes

    def import_from_csv(self, path):
        path = os.path.join(path, "")
        all_csv_files = [i for i in glob.glob(path + "**/*.csv", recursive=True)]
        if not (dataframes := self.read_csv(all_csv_files)):
            return
        combined_csv = pd.concat(dataframes)
        self.parse_rows(path, combined_csv)
