import itertools
import json
import operator
import os
import pickle
import urllib.request
from enum import Enum
from functools import reduce

import numpy as np
import osmium as osm
import pandas as pd
import shapely.wkb as wkblib
from pandas.core.common import flatten
from shapely import geometry


class Smoothness(Enum):
    # https://wiki.openstreetmap.org/wiki/Key:smoothness
    excellent = 0
    good = 1
    intermediate = 2
    bad = 3
    very_bad = 4
    horrible = 5
    very_horrible = 6
    impassable = 7


class Surface(Enum):
    # This is just a subset of available OSM Surface Values https://wiki.openstreetmap.org/wiki/Key:surface
    paved = 0
    asphalt = 1
    concrete = 2
    paving_stones = 3
    sett = 4
    unhewn_cobblestone = 5
    cobblestone = 6
    wood = 7


class OSMHandler(osm.SimpleHandler):
    wkbfab = osm.geom.WKBFactory()

    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.node_refs_data = []
        self.node_data = []
        self.way_data = []

    def node(self, n):
        elem = n
        for tag in elem.tags:
            self.node_data.append(["node",
                                   elem.id,
                                   elem.version,
                                   elem.visible,
                                   elem.location.lon,
                                   elem.location.lat,
                                   pd.Timestamp(elem.timestamp),
                                   elem.uid,
                                   len(elem.tags),
                                   tag.k,
                                   tag.v])

    def way(self, w):
        elem = w
        try:
            wkb = self.wkbfab.create_linestring(w)
            line = wkblib.loads(wkb, hex=True)
            # TODO add nodes of way with connection to
            for node in w.nodes:
                foo = node
                self.node_refs_data.append(["node_ref",
                                            foo.ref,
                                            elem.id,
                                            foo.x,
                                            foo.y])
            for tag in elem.tags:
                self.way_data.append(["way",
                                      elem.id,
                                      elem.version,
                                      elem.visible,
                                      wkb,
                                      line,
                                      pd.Timestamp(elem.timestamp),
                                      elem.uid,
                                      len(elem.tags),
                                      tag.k,
                                      tag.v])
        except RuntimeError:
            pass
            # print(elem.id)

    def create_dfs(self):
        node_columns = ["type", "id", "version", "visible", "lon",
                        "lat", "timestamp", "uid", "ntags", "tagkey", "tagvalue"]
        columns = ["type", "id", "version", "visible", "wkb",
                   "line", "timestamp", "uid", "ntags", "tagkey", "tagvalue"]
        node_refs_columns = ["type", "id",  "parent_id",  "x", "y"]

    #        relations = pd.DataFrame(self.relations_data, columns=columns)
        ways = pd.DataFrame(self.way_data, columns=columns)
        nodes = pd.DataFrame(self.node_data, columns=node_columns)
        node_refs = pd.DataFrame(
            self.node_refs_data, columns=node_refs_columns)

        return (node_refs, ways, nodes)


class OSMHelper:
    _file_name = "osm_cache_ways.json"
    _file_name_nodes = "osm_cache_nodes.json"
    _file_name_refs = "osm_cache_refs.json"

    def __init__(self, osm_file_path, cache_path, force_reload=False):
        self.osm_file_path = osm_file_path
        # TODO: add osm_file_path to cache_path
        self.cache_path = os.path.join(cache_path, self._file_name)
        self.cache_path_nodes = os.path.join(cache_path, self._file_name_nodes)
        self.cache_path_refs = os.path.join(cache_path, self._file_name_refs)
        self._load_ways(force_reload=force_reload)

    @property
    def _cache_exists(self):
        return os.path.exists(self.cache_path)

    def _load_ways(self, force_reload=False):
        if (not self._cache_exists) or force_reload:
            print("Ways not cached. Reloading..")
            osmhandler = OSMHandler()
            osmhandler.apply_file(self.osm_file_path,
                                  locations=True, idx='sparse_mem_array')
            dfs = osmhandler.create_dfs()

            self.ways_df = dfs[1]
            self.nodes_df = dfs[2]
            self.nodes_refs_df = dfs[0]
            #nodes = dfs[2]

            self.ways_df.to_pickle(self.cache_path)
            self.nodes_df.to_pickle(self.cache_path_nodes)
            self.nodes_refs_df.to_pickle(self.cache_path_refs)
        else:
            self.ways_df = pd.read_pickle(self.cache_path)
            self.nodes_df = pd.read_pickle(self.cache_path_nodes)
            self.nodes_refs_df = pd.read_pickle(self.cache_path_refs)

    def tags(self, osm_node_ids):
        result = {}
        for node in osm_node_ids:
            for way_id in self.nodes_refs_df[self.nodes_refs_df["id"] == node]["parent_id"].tolist():
                result.update(self.way_tags(way_id))
        return result

    def way_tags(self, osm_id):
        result = {}
        for index, row in self.ways_df[self.ways_df["id"] == osm_id].iterrows():
            result[row["tagkey"]] = row["tagvalue"]
        return result

    def get_distances_for_ways(self, ways_input, lat, lon):
        point = geometry.Point(lat, lon)
        unique_ways = ways_input[["id", "line"]].drop_duplicates(subset="id")

        unique_ways['distance'] = unique_ways['line'].apply(
            lambda x: x.distance(point))
        unique_ways = unique_ways.sort_values('distance')
        return unique_ways

    def get_filtred_tags_for_nodeids(self, nodeids):
        result = self.tags(nodeids)
        return {k: v for (k, v) in result.items() if "surface" in k or "smoothness" in k}

    def paginated_annotate_coordinates(self, input_coordinates, max_page_size=100):
        # We have to use pagination because many rides have to many coordinates to process them with one request
        # Not sure, but maybe the accuracy could be approved by using overlapping pages
        page_count = 1
        if len(input_coordinates) > max_page_size:
            page_count = (len(input_coordinates) % max_page_size) + 1
        result = []
        for paginated_coordinates in np.array_split(input_coordinates, page_count):
            output = self.annotate_coordinates(paginated_coordinates)
            result = list(itertools.chain(result, output))
        return result

    def annotate_coordinates(self, input_coordinates):
        # TODO improve docker handling
        url = "http://127.0.0.1:5000/match/v1/bike/"
        full_url = url + ";".join(map(lambda x: str(x[1])+","+str(
            x[0]), input_coordinates))+"?annotations=nodes&geometries=geojson&steps=true"
        response = None
        try:
            response = urllib.request.urlopen(full_url).read()
            content = json.loads(response)
        except Exception as e:
            print("loading annotations for url {} failed with error {} \n {}".format(
                full_url, response, e))
            return input_coordinates

        def tags_for_matching(matching):
            legs = matching["legs"]
            return list(map(lambda leg: self.get_filtred_tags_for_nodeids(
                leg["annotation"]["nodes"]), legs))

        matchings = list(
            map(lambda x: tags_for_matching(x), content["matchings"]))
        result = []

        for trace_point in content["tracepoints"]:
            if trace_point is None:
                result.append(trace_point)
            else:
                waypoint_index = trace_point["waypoint_index"]
                matchings_index = trace_point["matchings_index"]
                tags = matchings[matchings_index]
                if waypoint_index != 0 and waypoint_index < len(tags):
                    result.append(tags[waypoint_index])
                elif waypoint_index > len(tags):
                    print("tagscount: {}, index {}".format(
                        len(tags), trace_point["waypoint_index"]))
                    raise Exception("tags indices don't match")
                else:
                    result.append({"page-split": True})

        return list(zip(input_coordinates, result))

    def annotate_ride(self, ride):
        input_df = ride.ride_df[["lat", "lon"]].dropna()
        coordinates = list(zip(input_df.lat.tolist(), input_df.lon.tolist()))
        annotations = self.paginated_annotate_coordinates(coordinates)
        df = ride.ride_df

        smoothness_array = np.empty(len(df.index), dtype=object)
        smoothness_array[:] = np.nan

        surface_array = np.empty(len(df.index), dtype=object)
        surface_array[:] = np.nan

        row_index = 0
        coordinate_index = 0

        for index, row in df.iterrows():
            surface = np.nan
            smoothness = np.nan
            if np.isnan(row["lat"]) == False:
                tags = annotations[coordinate_index][1]
                if tags and type(tags) is dict:

                    def key_prio(dict, keys, tail):
                        for key in keys:
                            full_key = key+tail
                            if full_key in dict:
                                return dict[full_key]

                    # There are multiple possible keys that could contain the value we are interested in so we are checking for the most probable keys first
                    surface = key_prio(
                        tags, ["cycleway:right:", "cycleway:left:", ""], "surface")
                    smoothness = key_prio(
                        tags, ["cycleway:right:", "cycleway:left:", ""], "smoothness")

                surface_array[row_index] = surface
                smoothness_array[row_index] = smoothness
                coordinate_index += 1

            row_index += 1

        ride.ride_df["smoothness"] = smoothness_array
        ride.ride_df["surface"] = surface_array
