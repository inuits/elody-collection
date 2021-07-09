import glob
import os
import pandas as pd
import requests


class Importer:
    def __init__(self, storage):
        self.storage_api_url = os.getenv("STORAGE_API_URL", "http://localhost:8001")
        self.mount_point = os.getenv("UPLOAD_FOLDER", "")
        self.storage = storage

    def import_from_csv(self, path):
        path = os.path.join(path, "")
        all_csv_files = [i for i in glob.glob(path + "**/*.csv", recursive=True)]
        dataframes = []
        for f in all_csv_files:
            df = pd.read_csv(f)
            df.columns = df.columns.str.capitalize()
            dataframes.append(df)
        combined_csv = pd.concat(dataframes)
        for index, row in combined_csv.iterrows():
            if pd.isna(row["Padnaam"]) and pd.isna(row["Bestandsnaam"]):
                continue
            elif pd.isna(row["Padnaam"]):
                file_path = sorted(
                    glob.glob(path + "**/" + row["Bestandsnaam"], recursive=True)
                )[0]
            else:
                if row["Padnaam"][1] == ":":
                    file_path = str.replace(row["Padnaam"][3:], "\\", "/")
                else:
                    file_path = row["Padnaam"]
                file_path = os.path.join(self.mount_point, file_path)
            file_name = os.path.basename(file_path)
            object_id = row["Objectnummer"]
            mediafile = self.create_mediafile(object_id, file_name)
            metadata = self.add_metadata_to_entity(
                object_id, row["Rechtenstatus"], row["Copyright"]
            )
            upload_location = "{}/upload/{}".format(self.storage_api_url, file_name)
            self.upload_file(upload_location, file_path)

    def upload_file(self, upload_location, file_path):
        files = {"file": open(file_path, "rb")}
        requests.post(upload_location, files=files)

    def create_mediafile(self, object_id, file_name):
        location = {
            "location": "{}/download/{}".format(self.storage_api_url, file_name)
        }
        mediafile = self.storage.save_item_to_collection("mediafiles", location)
        ret = self.storage.add_mediafile_to_entity(
            "entities", object_id, mediafile["_id"]
        )
        if not ret:
            content = {"identifiers": object_id, "type": "asset"}
            self.storage.save_item_to_collection("entities", content)
            ret = self.storage.add_mediafile_to_entity(
                "entities", object_id, mediafile["_id"]
            )
        return ret

    def add_metadata_to_entity(self, object_id, rights, copyright):
        if pd.isna(rights) and pd.isna(copyright):
            return None
        if not pd.isna(rights):
            rights_obj = {"key": "rights", "value": rights}
            metadata = self.storage.update_collection_item_metadata_key("entities", object_id, "rights", rights_obj)
            if not metadata:
                metadata = self.storage.add_collection_item_metadata("entities", object_id, rights_obj)
        if not pd.isna(copyright):
            copyright_obj = {"key": "copyright", "value": copyright}
            metadata = self.storage.update_collection_item_metadata_key("entities", object_id, "copyright", copyright_obj)
            if not metadata:
                metadata = self.storage.add_collection_item_metadata("entities", object_id, copyright_obj)
        return metadata
