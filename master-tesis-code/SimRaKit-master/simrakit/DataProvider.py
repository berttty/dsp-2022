import bz2
import os
import pickle
import zipfile

from git import RemoteProgress, Repo

from .ProfileParser import Profile
from .RideParser import Ride


class DataProvider:

    def __init__(self, local_repo_path, dataset_dataset_git_url="https://github.com/simra-project/dataset.git"):
        self.local_repo_path = local_repo_path
        self.dataset_git_url = dataset_dataset_git_url

    def load_compressed(self, file_name, dir="processed_data"):
        full_name = None
        if dir is None:
            full_name = os.path.join(self.local_repo_path, file_name)
        else:
            full_name = os.path.join(self.local_repo_path, dir, file_name)

        file = bz2.open(full_name, 'r')
        object = pickle.load(file)
        file.close()
        return object

    def save_compressed(self, data, file_name, dir="processed_data", overwrite=True):
        full_name = None
        if dir is None:
            full_name = os.path.join(self.local_repo_path, file_name)
        else:
            full_dir = os.path.join(self.local_repo_path, dir)
            if not os.path.exists(full_dir):
                os.makedirs(full_dir)
            full_name = os.path.join(full_dir, file_name)

        if not overwrite:
            full_name = DataProvider.unique_file_name(
                full_dir, file_name, "zip")

        pickle.dump(data, bz2.open(full_name,  'wb'))

    @staticmethod
    def unique_file_name(path, file_name, extension):
        attempt_str = ""
        attempt_count = 1
        full_name = os.path.join(path, "{}{}.{}".format(
            file_name, attempt_str, extension))

        while os.path.exists(full_name):
            attempt_str = str(attempt_count)
            full_name = os.path.join(path, "{}{}.{}".format(
                file_name, attempt_str, extension))
            attempt_count += 1

        return full_name

    @staticmethod
    def load_compressed_static(file_name, base_dir, path="processed_data"):
        full_name = None
        if path is None:
            full_name = os.path.join(base_dir, file_name)
        else:
            full_name = os.path.join(base_dir, path, file_name)

        if not os.path.exists(full_name):
            return None

        file = bz2.open(full_name, 'r')
        object = pickle.load(file)
        file.close()
        return object

    @staticmethod
    def save_compressed_static(data, file_name, base_dir, path):
        full_name = None
        if path is None:
            full_name = os.path.join(base_dir, file_name)
        else:
            full_dir = os.path.join(base_dir, path)
            if not os.path.exists(full_dir):
                os.makedirs(full_dir)
            full_name = os.path.join(full_dir, file_name)

        pickle.dump(data, bz2.open(full_name,  'wb'), protocol=4)

    def clone_repo(self, force=False):
        # TODO: replace clone with download https://github.com/simra-project/dataset/archive/master.zip
        if not(os.path.isdir(self.local_repo_path) and os.listdir(self.local_repo_path)) or force:
            class CloneProgress(RemoteProgress):
                def update(self, op_code, cur_count, max_count=None, message=''):
                    if message:
                        print(message, cur_count, max_count, op_code, message)

            Repo.clone_from(self.dataset_git_url, self.local_repo_path,
                            branch='master', progress=CloneProgress())
        else:
            print("{} already contains files. Skipping clone".format(
                self.local_repo_path))

    def unzip_data(self, city="Berlin", force=False):
        target_dir = os.path.join(self.local_repo_path, city + "Unzipped")
        city_dir = os.path.join(self.local_repo_path, city)

        profiles_path = os.path.join(city_dir, "Profiles", "Profiles.zip")
        rides_dir = os.path.join(city_dir, "Rides")

        if not (os.path.isdir(target_dir) and os.listdir(target_dir)) or force:

            with zipfile.ZipFile(profiles_path, "r") as zip_ref:
                zip_ref.extractall(os.path.join(target_dir, "Profiles"))

            for ride in os.listdir(rides_dir):
                ride_path = os.path.join(rides_dir, ride)
                with zipfile.ZipFile(ride_path, "r") as zip_ref:
                    zip_ref.extractall(os.path.join(target_dir, "Rides"))

            # TODO: clean up zip after unzipping
        else:
            print("{} already contains files. Skipping unzip".format(target_dir))

    def clone_and_unzip(self,  force=False):
        # TODO add support for more cities?
        self.clone_repo(force=force)
        self.unzip_data("Berlin", force=force)

    def list_ride_ids(self, city="Berlin"):
        target_dir = os.path.join(self.local_repo_path, city + "Unzipped")
        rides_dir = os.path.join(target_dir, "Rides")
        return os.listdir(rides_dir)

    def list_profile_ids(self, city="Berlin"):
        target_dir = os.path.join(self.local_repo_path, city + "Unzipped")
        profiles_dir = os.path.join(target_dir, "Profiles")
        return os.listdir(profiles_dir)

    def get_all_rides(self, filter_lambda=None, city="Berlin"):
        all_rides = list(map(lambda x: self.get_ride_data(
            x, city=city), self.list_ride_ids(city=city)))
        if filter_lambda is None:
            return all_rides
        return list(filter(filter_lambda, all_rides))

    def get_all_profiles(self, filter_lambda=None, city="Berlin"):
        all_profiles = list(map(lambda x: self.get_profile_data(
            x, city=city), self.list_profile_ids(city=city)))
        if filter_lambda is None:
            return all_profiles
        return list(filter(filter_lambda, all_profiles))

    def get_ride_data(self, ride_id, city="Berlin"):
        target_dir = os.path.join(self.local_repo_path, city + "Unzipped")
        rides_dir = os.path.join(target_dir, "Rides")
        return Ride.from_file(os.path.join(rides_dir, ride_id))

    def get_profile_data(self, profile_id, city="Berlin"):
        target_dir = os.path.join(self.local_repo_path, city + "Unzipped")
        profiles_dir = os.path.join(target_dir, "Profiles")
        return Profile.from_file(os.path.join(profiles_dir, profile_id))
