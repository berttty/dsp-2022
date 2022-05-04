from io import StringIO
from enum import Enum

import pandas as pd

class BirthYearGroup(Enum):
    UNKOWN = -1
    NOTCHOSEN = 0  # default
    AFTER2004 = 1
    BETWEEN_2000_2004 = 2
    BETWEEN_1995_1999 = 3
    BETWEEN_1990_1994 = 4
    BETWEEN_1985_1989 = 5
    BETWEEN_1980_1984 = 6
    BETWEEN_1975_1979 = 7
    BETWEEN_1970_1974 = 8
    BETWEEN_1965_1969 = 9
    BETWEEN_1960_1964 = 10
    BETWEEN_1955_1959 = 11
    BETWEEN_1950_1954 = 12
    BEFORE_1950 = 13

    @classmethod
    def values(cls):
        return list(map(lambda x: x.value, cls._member_map_.values()))

    @classmethod
    def description_titles(cls):
        return ["Unkown", "Not choosen", "After 2004", "2000-2004", "1995-1999", "1990-1994", "1985-1989", "1980-1984", "1975-1979", "1970-1974", "1965-1969", "1960-1964", "1955-1959", "1950-1954", "Before 1950"]
    

class Gender(Enum):
    UNKOWN = -1
    NOTCHOSEN = 0  # default
    MALE = 1
    FEMALE = 2
    OTHER = 3

    @classmethod
    def values(cls):
        return list(map(lambda x: x.value, cls._member_map_.values()))

    @classmethod
    def description_titles(cls):
        return ["Unkown", "Not choosen", "Male", "Female", "Other"]

class Experience(Enum):
    UNKOWN = -1
    NOTCHOSEN = 0  # default
    MORETHAN10 = 1
    BETWEEN5_10 = 2
    BETWEEN2_4 = 3
    LESSTHAN2 = 4

    @classmethod
    def values(cls):
        return list(map(lambda x: x.value, cls._member_map_.values()))

    @classmethod
    def description_titles(cls):
        return ["Unkown", "Not choosen", "More than 10", "5 - 10", "2-4", "Less than 2"]
    

# TODO add region ENUM
# region:
# 0 = Please Choose (default value)
# 1 = Berlin
# 2 = London
# 3 = Other
# 4 = Bern
# 5 = Pforzheim/Enzkreis
# 6 = Augsburg
# 7 = Ruhr Region
# 8 = Stuttgart
# 9 = Leipzig
# 10 = Wuppertal/Solingen/Remscheid
# 11 = Düsseldorf


# birth,gender,region,experience,numberOfRides,duration,numberOfIncidents,waitedTime,distance,co2,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,behaviour,numberOfScary
class Profile(object):

    _cached_df = None

    def __init__(self, path, title):
        self.path = path
        self.title = title

    @staticmethod
    def from_file(path):
        title = str(path).split("/")[-1]
        return Profile(path, title)

    @property
    def df(self):
        if self._cached_df is None:
            self._cached_df = _parse_df(self.path)
        return self._cached_df

    @property
    def number_of_rides(self):
        if "numberOfRides" in self.df:
            return self.df["numberOfRides"].iloc[0]
        else:
            # print(self.title)
            return -1
    
    @property
    def birth_year(self):
        if "birth" in self.df:
            return self.df["birth"].iloc[0]
        else:
            # print(self.title)
            return -1

    @property
    def gender(self):
        if "gender" in self.df:
            return self.df["gender"].iloc[0]
        else:
            # print(self.title)
            return -1

    @property
    def experience(self):
        if "experience" in self.df:
            return self.df["experience"].iloc[0]
        else:
            # print(self.title)
            return -1


def _parse_df(path):
    components = _get_profile_file_components(path)
    csv_data = components[1]


    return pd.read_csv(StringIO(csv_data))


def _get_profile_file_components(path):
    with open(path, "r") as raw_file:
        raw_data = raw_file.read().splitlines(True)
        return [raw_data[0], "".join(raw_data[1:])]
    return None
