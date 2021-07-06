import glob
import os
import pandas as pd
import requests


class Importer:
    def __init__(self, storage):
        self.storage_api_url = os.getenv("STORAGE_API_URL", "http://localhost:8001")
        self.mount_point = os.getenv("MOUNT_POINT", "")
        self.storage = storage

    def import_from_csv(self, path):
        path = os.path.join(path, "")
        all_csv_files = [i for i in glob.glob(path + "**/*.csv", recursive=True)]
        combined_csv = pd.concat([pd.read_csv(f) for f in all_csv_files])
        for index, row in combined_csv.iterrows():
            if pd.isna(row["Padnaam"]) and pd.isna(row["Bestandsnaam"]):
                continue
            elif pd.isna(row["Padnaam"]):
                file_path = sorted(
                    glob.glob(path + "**/" + row["Bestandsnaam"], recursive=True)
                )[0]
            else:
                if ":" in row["Padnaam"]:
                    file_path = str.replace(row["Padnaam"][3:], "\\", "/")
                else:
                    file_path = row["Padnaam"]
                file_path = self.mount_point + file_path
            file_name = os.path.basename(file_path)
            object_id = row["Objectnummer"]
            mediafile = self.create_mediafile(object_id, file_name)
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
        self.storage.add_mediafile_to_entity("entities", object_id, mediafile["_id"])
        return mediafile
