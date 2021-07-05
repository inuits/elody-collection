import glob
import os
import pandas as pd
import requests


class Importer:
    def __init__(self, storage):
        self.storage_api_url = os.getenv("STORAGE_API_URL", "http://localhost:8001")
        self.storage = storage

    def import_from_csv(self, path):
        path = os.path.join(path, '')
        all_csv_files = [i for i in glob.glob(path + "**/*.csv", recursive=True)]
        combined_csv = pd.concat([pd.read_csv(f) for f in all_csv_files])
        for index, row in combined_csv.iterrows():
            if pd.isna(row["Padnaam"]):
                file_name = sorted(glob.glob(path + "**/" + row["Bestandsnaam"], recursive=True))[0]
            else:
                # FIXME: this will eventually need to be changed to point to our mount point
                file_name = row["Padnaam"]
            object_id = row["Objectnummer"]
            mediafile = self.create_mediafile(object_id, file_name)
            upload_location = "{}/upload/{}".format(self.storage_api_url, file_name)
            self.upload_file(upload_location, "{}/{}".format(path, file_name))

    def upload_file(self, upload_location, filename):
        files = {"file": open(filename, "rb")}
        requests.post(upload_location, files=files)

    def create_mediafile(self, object_id, file_name):
        location = {
            "location": "{}/download/{}".format(self.storage_api_url, file_name)
        }
        mediafile = self.storage.save_item_to_collection("mediafiles", location)
        self.storage.add_mediafile_to_entity("entities", object_id, mediafile["_id"])
        return mediafile
