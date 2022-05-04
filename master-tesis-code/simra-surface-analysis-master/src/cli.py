from SimRaKit.RideParser import Ride
from ThresholdBasedClassifier import ThresholdBasedClassifier
from SimRaKit.DataProvider import DataProvider
import os


class CLI(object):

    def generate_result_for_rides(self, rides, output_dir):
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        for ride in rides:
            ride.interpolate_locations()

            ride.ride_df['result'] = ThresholdBasedClassifier(
                100).classify(ride)
            gdf = ride.as_GeoDataFrame(["result"])
            total_output_path = os.path.join(output_dir, ride.title + ".geojson")
            gdf.to_file(total_output_path, driver='GeoJSON')

    def generate_result(self, input_path, output_dir):
        inputs = []
        if os.path.isdir(input_path):
            for file_name in os.listdir(input_path):
                total_path = os.path.join(input_path, file_name)
                inputs.append(total_path)
        else:
            inputs.append(input_path)

        rides = list(map(lambda x: Ride.from_file(x), inputs))
        self.generate_result_for_rides(rides, output_dir)
