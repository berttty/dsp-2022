import numpy as np
import pandas as pd
import os

from SimRaKit.RideParser import Ride
from SignalUtility import *


class ThresholdBasedClassifier(object):
    @staticmethod
    def generate_result_for_rides(rides, output_dir):
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        for ride in rides:
            ride.interpolate_locations()

            ride.ride_df['result'] = ThresholdBasedClassifier().classify(ride)
            gdf = ride.as_GeoDataFrame(["result"])
            total_output_path = os.path.join(
                output_dir, ride.title + ".geojson")
            gdf.to_file(total_output_path, driver='GeoJSON')

    @staticmethod
    def generate_result(input_path, output_dir):
        inputs = []
        if os.path.isdir(input_path):
            for file_name in os.listdir(input_path):
                total_path = os.path.join(input_path, file_name)
                inputs.append(total_path)
        else:
            inputs.append(input_path)

        rides = list(map(lambda x: Ride.from_file(x), inputs))
        ThresholdBasedClassifier.generate_result_for_rides(rides, output_dir)

    def relative_scale(self, df):
        max = df[["X", "Y", "Z"]].max().max()
        min = abs(df[["X", "Y", "Z"]].min().min())
        if min > max:
            return min
        else:
            return max

    def classify(self, ride):
        fs = ride.frequency_for_column("X")
        df = ride.ride_df
        cut_off = 1

        buckets = ride.split_by_interruption(clip=20)
        if len(buckets) == 0:
            buckets = [ride.ride_df]

        scale_df = pd.concat(buckets)
        print("----")
        print(self.relative_scale(ride.ride_df))
        print(self.relative_scale(scale_df))

        df["X"] = scale(df["X"], 35)
        df["Y"] = scale(df["Y"], 35)
        df["Z"] = scale(df["Z"], 35)

        input = df[["X", "Y", "Z"]].to_numpy()
        transformed = np.transpose(fast_ICA(input, 1))

        df["ICA"] = transformed[0] * 10
        df["ICA_low"] = low_pass_filter(values=df.ICA, fs=fs, cut_off=cut_off)
        df[["ICA_var"]] = df[["ICA_low"]].rolling(window=10).var()

        return pd.np.digitize(df.ICA_var, bins=[0.0, 0.1, 0.2, 0.4, 0.8])
