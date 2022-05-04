import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from functools import reduce
from operator import add
import multiprocessing
from shapely.geometry import LineString, Point, box

class BoundingBox(object):
    def __init__(self, min_lon, max_lon, min_lat, max_lat, title = "Unkown"):
        self.min_lon = min_lon
        self.max_lon = max_lon
        self.min_lat = min_lat
        self.max_lat = max_lat

        self.lon_size = max_lon-min_lon
        self.lat_size = max_lat-min_lat

        self.lon_center = (min_lon+max_lon)/2
        self.lat_center = (min_lat+max_lat)/2

        self.title = title

    def split_into_four_parts(self):
        lon_part_size = (self.max_lon - self.min_lon)/4
        lat_part_size = (self.max_lat - self.min_lat)/4

        return [
            BoundingBox(self.min_lon,  self.min_lon+lon_part_size, self.min_lat, self.min_lat+lat_part_size),
            BoundingBox(self.min_lon+lon_part_size,  self.max_lon, self.min_lat, self.min_lat+lat_part_size),
            BoundingBox(self.min_lon,  self.min_lon+lon_part_size, self.min_lat+lat_part_size, self.max_lat),
            BoundingBox(self.min_lon+lon_part_size,  self.max_lon, self.min_lat+lat_part_size, self.max_lat)
        ]

    @staticmethod
    def box(center_lon, center_lat, degrees, title="Unkown"):
        d = degrees/2
        return BoundingBox(center_lon - degrees, center_lon + degrees,  center_lat - d, center_lat + d, title)


def _matches_for_bounding_box(df, bounding_box):
    return df[(df['lat'] >= bounding_box.min_lat) & (df['lat'] <= bounding_box.max_lat) & (df['lon'] >= bounding_box.min_lon) & (df['lon'] <= bounding_box.max_lon)]
# https://gis.stackexchange.com/a/8674/164949

# decimal
# places   degrees          distance
# -------  -------          --------
# 0        1                111  km
# 1        0.1              11.1 km
# 2        0.01             1.11 km
# 3        0.001            111  m
# 4        0.0001           11.1 m
# 5        0.00001          1.11 m
# 6        0.000001         11.1 cm
# 7        0.0000001        1.11 cm
# 8        0.00000001       1.11 mm

def fast_cluster(df, longitude_cluster_size = 0.0002, latitude_cluster_size = 0.0001, bounding_box = None):
    df = df[["lon", "lat", "label"]].dropna()
    if bounding_box is None:
        bounding_box = BoundingBox(df.lon.dropna().min(), df.lon.dropna().max(), df.lat.dropna().min(), df.lat.dropna().max())

    result = []

    # latitude_cluster_size = lat_degrees_for_meter(11.1, bounding_box.lon_center)
    candidates = [{"bBox":bounding_box, "df":df}]

    while(len(candidates) > 0):
        new_candidates = []
        for candidate in candidates:
            bBox = candidate["bBox"]
            current_df = candidate["df"]

            matches =  _matches_for_bounding_box(current_df, bBox)
            if len(matches > 0):
                new_parts = bBox.split_into_four_parts()
                if new_parts[0].lon_size < longitude_cluster_size or new_parts[0].lat_size < latitude_cluster_size:
                    result = result + cluster(current_df, longitude_cluster_size=longitude_cluster_size, latitude_cluster_size=latitude_cluster_size, bounding_box=bBox)
                else:
                    for part in new_parts:
                        new_candidates.append({"bBox":part, "df":matches})
                                
        candidates = new_candidates
    return result

def cluster(df, longitude_cluster_size = 0.01, latitude_cluster_size = 0.01, bounding_box = None):
    df = df[["lon", "lat", "label"]].dropna()

    min_lon = None
    max_lon = None
    min_lat = None
    max_lat = None

    if bounding_box is None:
        min_lon = df.lon.dropna().min()
        max_lon = df.lon.dropna().max()
        min_lat = df.lat.dropna().min()
        max_lat = df.lat.dropna().max()
    else:
        min_lon = bounding_box.min_lon
        max_lon = bounding_box.max_lon
        min_lat = bounding_box.min_lat
        max_lat = bounding_box.max_lat

    result = []

    for lon in np.arange(min_lon, max_lon, longitude_cluster_size):
        current_min_lon = lon - longitude_cluster_size/2
        current_max_lon = lon + longitude_cluster_size/2

        for lat in np.arange(min_lat, max_lat, latitude_cluster_size):
            current_min_lat = lat - latitude_cluster_size/2
            current_max_lat = lat + latitude_cluster_size/2
            #TODO: because we are using >= and <= rows could end up in multiple buckets not sure if we want that
            matches = df[(df['lat'] >= current_min_lat) & (df['lat'] <= current_max_lat) & (df['lon'] >= current_min_lon) & (df['lon'] <= current_max_lon)]

            if len(matches) > 0:
                result.append({"centroid": (lon, lat), "elements": matches, "box" : box(lon,  lat, lon+longitude_cluster_size, lat + latitude_cluster_size)})
    return result