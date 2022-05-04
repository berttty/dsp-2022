from enum import Enum
from io import StringIO

import geopy.distance
import numpy as np
import pandas as pd
import shapely.wkt
from geopandas import GeoDataFrame
from shapely.geometry import LineString, Point


class BicycleType(Enum):
    NOTCHOSEN = 0  # default
    CITYTREKKINGBIKE = 1
    ROADRACINGBIKE = 2
    EBIKE = 3
    RECUMBENTBICYCLE = 4
    FREIGHTBICYCLE = 5
    TANDEMBICYCLE = 6
    MOUNTAINBIKE = 7
    OTHER = 8
    UNKOWN = -1


class PhoneLocation(Enum):
    POCKET = 0  # default
    HANDLEBAR = 1
    JACKETPOCKET = 2
    HAND = 3
    BASKETPANNIER = 4
    BACKPACKBAG = 5
    OTHER = 6
    UNKOWN = -1


class IncidedntType(Enum):
    NOTHING = 0  # default
    CLOSEPASS = 1
    SOMEONEPULLINGINOROUT = 2
    NEARLEFTORRIGHTHOOK = 3
    SOMEONEAPPROACHINGHEADON = 4
    TAILGATING = 5
    NEARDOORING = 6
    DODGINGANOBSTACLE = 7
    OTHER = 8
    UNKOWN = -1


class Ride(object):

    _cached_incidents_df = None
    _cached_ride_df = None

    def __init__(self, path, title):
        self.path = path
        self.title = title

    @staticmethod
    def from_file(path):
        title = str(path).split("/")[-1]
        return Ride(path, title)

    @property
    def incidents_df(self):
        if self._cached_incidents_df is None:
            self._cached_incidents_df = _parse_incidents_df(self.path)
        return self._cached_incidents_df

    @property
    def ride_df(self):
        if self._cached_ride_df is None:
            self._cached_ride_df = _parse_ride_df(self.path)
        return self._cached_ride_df

    @property
    def bicycle_type(self):
        if self.incidents_df.empty:
            return BicycleType.UNKOWN
        return BicycleType(self.incidents_df.bike.iloc[0])

    @property
    def phone_location(self):
        if self.incidents_df.empty:
            return PhoneLocation.UNKOWN
        return PhoneLocation(self.incidents_df.pLoc.iloc[0])

    @property
    def start_time(self):
        if self.is_empty:
            return None
        return self.ride_df.head(1).index.values[0]

    @property
    def end_time(self):
        if self.is_empty:
            return None
        return self.ride_df.tail(1).index.values[0]

    @property
    def duration(self):
        if self.is_empty:
            return 0
        return pd.Timedelta(self.end_time - self.start_time).total_seconds()

    @property
    def is_empty(self):
        return len(self.ride_df.index) == 0

    def split_by_interruption(self, bucket_min_distance=None, clip = 0):
        if 'speed' not in self.ride_df.columns:
            self.insert_geo_features()
        df = self.ride_df

        buckets = []
        previous_index = 0


        for index in df.index[df['speed'] == 0].tolist():
            integer_location = np.where(df.index == index)[0][0]

            if len(df) < integer_location-clip:
                continue
            current_sub = df[previous_index:integer_location].copy()

            if clip != 0:
                current_sub = current_sub.head(int(clip/2))
                current_sub = current_sub.tail(-int(clip/2))
            
            current_bucket_distance = current_sub['distance'].dropna().sum()
            if current_bucket_distance > 0:
                if bucket_min_distance is None or current_bucket_distance > bucket_min_distance:
                    buckets.append(current_sub)

            previous_index = integer_location

        return buckets

    def frequency_for_column(self, column_name):
        df = self.ride_df
        df = df[[column_name]].dropna()
        # TODO: add support for df.index.inferred_freq if data is continous
        try:
            min_diff = np.diff(df.index.values).min().astype(int)
            if min_diff != 0:
                return 1e9 / min_diff
            else:
                return None
        except Exception as e:
            print(e)
            return None

    def interpolate_locations(self):
        self.ride_df['lon'] = self.ride_df.lon.interpolate(method='linear', order=1)
        self.ride_df['lat'] = self.ride_df.lat.interpolate(method='linear', order=1)

    def insert_geo_features(self):
        previous_point = None
        previous_time = None
        df = self.ride_df

        speed_array = np.empty(len(df.index))
        speed_array[:] = np.nan

        distance_array = np.empty(len(df.index))
        distance_array[:] = np.nan

        linestring_array = np.empty(len(df.index), dtype=object)
        linestring_array[:] = np.nan

        row_index = 0

        for index, row in df.iterrows():
            current_point = (row["lat"], row["lon"])
            current_time = index
            if np.isnan(current_point[0]) == False:
                if previous_point is not None:
                    time_delta = pd.Timedelta(
                        current_time - previous_time).total_seconds()
                    dist = geopy.distance.geodesic(
                        current_point, previous_point)
                    speed = (dist.meters/time_delta)*3.6

                    distance_array[row_index] = dist.meters
                    speed_array[row_index] = speed
                    linestring_array[row_index] = LineString([Point(
                        previous_point[1], previous_point[0]), Point(current_point[1], current_point[0])]).wkt

                previous_point = current_point
                previous_time = current_time
            row_index += 1

        self.ride_df["speed"] = speed_array
        self.ride_df["distance"] = distance_array
        self.ride_df["linestring"] = linestring_array

    # TODO add properties for df columns
    def as_GeoDataFrame(self, columns_to_inlude=[]):
        if 'linestring' not in self.ride_df.columns:
            self.insert_geo_features()
        df = self.ride_df
        columns = ['linestring'] + columns_to_inlude
        df = df[columns].dropna()
        geometry = df['linestring'].map(shapely.wkt.loads)
        return GeoDataFrame(df, geometry=geometry)


def _remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def _remove_suffix(text, suffix):
    if text.endswith(suffix):
        return text[:-len(suffix)]
    return text


def _replace_suffix(text, suffix, replacement):
    if text.endswith(suffix):
        return text[:-len(suffix)] + replacement
    return text


def _clamp_text(text, prefix, suffix):
    result = text
    result = _remove_prefix(text, prefix)
    result = _remove_suffix(result, suffix)
    return result


def _get_ride_file_components(path):
    with open(path, "r") as raw_file:
        raw_data = raw_file.read()
        components = raw_data.split("=========================")
        if len(components) == 1:
            components = raw_data.split("===================")

        return components
    return None


def _try_to_recover_from_broken_csv(data):
    lines = data.split("\n")
    expected_number_of_columns = len(lines[0].split(","))
    for idx, line in enumerate(lines):
        column_delta = len(line.split(",")) - expected_number_of_columns
        if column_delta > 0:
            lines[idx] = _remove_suffix(line, "," * column_delta)
    return "\n".join(lines)


def _parse_incidents_df(path):
    components = _get_ride_file_components(path)

    raw_incidents_data = components[0]
    incident_number = raw_incidents_data.split("\n")[0]
    incidents_data = _clamp_text(
        raw_incidents_data, prefix=incident_number+"\n", suffix="\n\n")
    incidents_data = _try_to_recover_from_broken_csv(incidents_data)
    if not incidents_data:
        return pd.DataFrame()

    return pd.read_csv(StringIO(incidents_data))


def _parse_ride_df(path):
    components = _get_ride_file_components(path)

    raw_ride_data = components[1]
    ride_number = raw_ride_data.split("\n")[1]
    ride_data = _remove_prefix(raw_ride_data, "\n"+ride_number+"\n")

    ride_df = pd.read_csv(StringIO(ride_data))
    ride_df["timeStamp"] = pd.to_datetime(ride_df["timeStamp"], unit="ms")
    ride_df.set_index(pd.DatetimeIndex(ride_df["timeStamp"]), inplace=True)

    return ride_df
