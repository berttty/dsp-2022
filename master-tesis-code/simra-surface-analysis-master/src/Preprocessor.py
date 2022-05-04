import pandas as pd

from SimRaKit.DataProvider import DataProvider
from SimRaKit.RideParser import PhoneLocation, Ride

base_dir = "/Users/leonardthomas/simra-surface-analysis"
path = "preprocessed-data"

# Takes a list of rides and returns a list of filtred dataframes
# The key argument is a stringvalue that allows caching the result to disk


def process(rides, key=None, force_reload=False, interpolate_locations=True):
    if key is not None and force_reload == False:
        data = DataProvider.load_compressed_static(key, base_dir, path=path)
        if data is not None:
            return data
    result = []
    failures  = 0
    index = 0
    for ride in rides:

        index += 1
        ride_without_interruptions = None
        parts = None
        try:
            # we calculate geo features before we might interpolate locations so that split_by_interruptions doesn't consider interpolated loations
            ride.insert_geo_features()
            if interpolate_locations:
                ride.interpolate_locations()
            parts = ride.split_by_interruption()     
            ride_without_interruptions = pd.concat(parts)
        except:

            #print(ride.ride_df[["lat", "lon", "speed"]].dropna())
            # TODO: investiage empty buckets for non  empty rides
            failures += 1
            print(parts)
            print((index, failures))
            continue
        # acc = ride_without_interruptions[["X","Y","Z"]]
        # x_max = acc_abs["X"].max()
        # y_max = acc_abs["Y"].max()
        # z_max = acc_abs["Z"].max()
        for part in parts:
            df = part
            df["X_norm"] = df["X"].rolling(window=10).var()
            df["Y_norm"] = df["Y"].rolling(window=10).var()
            df["Z_norm"] = df["Z"].rolling(window=10).var()
            df["XYZ_norm_mean"] = df[["X_norm", "Y_norm", "Z_norm"]].mean(axis=1)
            result.append(df)

    if key is not None:
        DataProvider.save_compressed_static(result, key, base_dir, path)

    return result
