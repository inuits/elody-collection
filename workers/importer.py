import glob
import os
import pandas as pd
import requests


class Importer:
    def __init__(self, storage):
        self.storage_api_url = os.getenv("STORAGE_API_URL", "http://localhost:8001")
        self.mount_point = os.getenv("UPLOAD_FOLDER", "")
        self.storage = storage

    def upload_file(self, file_name, file_path):
        upload_location = "{}/upload/{}".format(self.storage_api_url, file_name)
        files = {"file": open(file_path, "rb")}
        requests.post(upload_location, files=files)

    def add_metadata_to_entity(self, object_id, rights, copyright):
        if pd.isna(rights) and pd.isna(copyright):
            return None
        new_metadata = []
        if not pd.isna(rights):
            new_metadata.append({"key": "rights", "value": rights})
        if not pd.isna(copyright):
            new_metadata.append({"key": "copyright", "value": copyright})
        all_metadata = self.storage.get_collection_item_metadata("entities", object_id)
        if all_metadata:
            if all(elem in all_metadata for elem in new_metadata):
                return all_metadata
            for index, data in enumerate(all_metadata):
                if ("key", "copyright") in data.items() and not pd.isna(copyright):
                    all_metadata[index] = new_metadata.pop()
                if ("key", "rights") in data.items() and not pd.isna(rights):
                    all_metadata[index] = new_metadata.pop()
        if new_metadata:
            all_metadata = all_metadata + new_metadata if all_metadata else new_metadata
        ret_metadata = self.storage.update_collection_item_metadata(
            "entities", object_id, all_metadata
        )
        return ret_metadata

    def create_mediafile(self, object_id, file_name):
        data = {
            "type": "mediafile",
            "location": "{}/download/{}".format(self.storage_api_url, file_name),
        }
        mediafile = self.storage.save_item_to_collection("mediafiles", data)
        ret = self.storage.add_mediafile_to_entity(
            "entities", object_id, mediafile["_id"]
        )
        if not ret:
            content = {"identifiers": [object_id], "type": "asset"}
            self.storage.save_item_to_collection("entities", content)
            ret = self.storage.add_mediafile_to_entity(
                "entities", object_id, mediafile["_id"]
            )
        return ret

    def write_to_db(self, object_id, file_name, file_path, row):
        mediafile = self.create_mediafile(object_id, file_name)
        metadata = self.add_metadata_to_entity(
            object_id, row["Rechtenstatus"], row["Copyright"]
        )
        self.upload_file(file_name, file_path)

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
            file_path = os.path.join(self.mount_point, file_path)
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
