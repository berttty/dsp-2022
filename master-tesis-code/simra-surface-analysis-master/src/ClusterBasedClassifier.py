import numpy as np
import pandas as pd
import shapely.wkt
from geopandas import GeoDataFrame

import GeoCluster
from SignalUtility import *
from SimRaKit.DataProvider import DataProvider
import Preprocessor

verbose = True
bounding_box = None

# runtime cache allows multiple classify operations on the same data without hitting the disk and decompression of the file
runtime_cache = {}

def _print(message):
    if verbose:
        print(message)


def classify(rides, preprocessing_key="cluter_based_preprocessing_2", export_name="result", force_reload = False, add_score_hist_data=False):
    cache_key = preprocessing_key+"cache"
    df = None
    
    if not force_reload:
        if cache_key in runtime_cache:
            df = runtime_cache[cache_key]
        else:
            df = DataProvider.load_compressed_static(cache_key, Preprocessor.base_dir, path=Preprocessor.path)
            
    if df is None:
        _print("starting preprocessing")
        dfs = []

        for df in Preprocessor.process(rides, key=preprocessing_key, force_reload=False):
            processed = preprocess(df)
            if processed is not None:
                dfs.append(processed)
        _print("finished preprocessing")
        df = pd.concat(dfs, sort=False)
        DataProvider.save_compressed_static(df, cache_key, Preprocessor.base_dir, path=Preprocessor.path)
    else:
        print("Hitting cached file, no preprocessing required")

    runtime_cache[cache_key] = df

    _print("starting clusterring")
    cluster = GeoCluster.fast_cluster(df, bounding_box=bounding_box)
    if len(cluster) == 0:
        print("no matches for given bounding box")
        return None
    _print("finished clusterring")
    _print("Analyzing")
    df = pd.DataFrame(list(map(lambda c: analyze_cluster(c, add_score_hist_data=add_score_hist_data), cluster))).dropna()

    _print("Saving")
    # Box is already in the geometry and locations is currently stored as a tupel with is not suported
    gdf_columns = df.drop(columns=["box", "location"])
    geometry = df['box']
    gdf = GeoDataFrame(gdf_columns, geometry=geometry)

    export_path = DataProvider.unique_file_name("/Users/leonardthomas/simra-surface-analysis", export_name, "geojson")
    gdf.to_file(export_path, driver='GeoJSON')

    dataprovider = DataProvider("data_cache")
    dataprovider.save_compressed(df, export_name, "cluster_result", overwrite=False)
    _print("Saved to {}".format(export_path))
    return df


def analyze_cluster(cluster, add_score_hist_data=False):
    df = cluster["elements"]
    centroid = cluster["centroid"]
    box = cluster["box"]
    result_value = int(df.label.pow(2).mean())
    # TODO investigate why that happens
    if result_value > 5 or result_value < 0:
        result_value = None
    result = {
        "location": centroid,
        "box": box,
        "mean": df.label.mean(),
        "median": df.label.median(),
        "std": df.label.std(),
        "count": len(df),
        "result": result_value,
    }

    result = {**result, **style_values_for_label(result_value)}

    if add_score_hist_data:
        result["1"] = len(df[df['label'] == 1])
        result["2"] = len(df[df['label'] == 2])
        result["3"] = len(df[df['label'] == 3])
        result["4"] = len(df[df['label'] == 4])
        result["5"] = len(df[df['label'] == 5])

    # _print(result)
    return result


def style_values_for_label(label):
    def color_for_label(label):
        if label is None:
            return '#000000'
        color_map = {
            1: '#0dff00',
            2: '#CCFF33',
            3: '#FFFF00',
            4: '#FF9900',
            5: '#FF0000'
        }
        return color_map[label]
    return {
        "fill-opacity": 1,
        "fill": color_for_label(label),
        "stroke": color_for_label(label),
        "fillOpacity": 1,
        "fillColor": color_for_label(label),
        "color": color_for_label(label),
        "opacity": 1,
        "weight": 3
    }


def preprocess(df):
    #df["XYZ_norm_mean_low"] = low_pass_filter(values=df.XYZ_norm_mean, fs=fs, cut_off=cut_off)
    # df["XYZ_norm_mean_var"] = df[["XYZ_norm_mean"]].rolling(window=10).var()
    if len(df["XYZ_norm_mean"].dropna()) == 0:
        return None

    df = df[df['XYZ_norm_mean'].notna()].copy()
    values = df["XYZ_norm_mean"].to_numpy()
    df["label"] = pd.cut(values, 5, labels=[
        1, 2, 3, 4, 5]).astype(int)
    if "a" in df.columns:
        df = df.drop(columns=["a", "b", "c"])
    return df
