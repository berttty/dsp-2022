import fire
import pandas as pd

from cli import CLI
from SimRaKit.DataProvider import DataProvider
from SimRaKit.DataSetStats import *
from SimRaKit.RideParser import PhoneLocation, Ride
from ThresholdBasedClassifier import ThresholdBasedClassifier
import ClusterBasedClassifier
from GeoCluster import BoundingBox


def evaluate_cluster_based():
    samples = [
        BoundingBox.box(13.416521, 52.532273, 0.0005, "1"),  # Straßburgerstr
        BoundingBox.box(13.362504, 52.514109, 0.0005, "2"),  # Bellevueallee
        BoundingBox.box(13.254794, 52.485049, 0.0005, "3"),  # Koenigswegs
        BoundingBox.box(13.363288, 52.515445, 0.0005, "4"),  # 17. Juni
        BoundingBox.box(13.334830, 52.513611, 0.0005, "5"),  # 17. Juni 2
        BoundingBox.box(13.453806, 52.509132, 0.0005, "6"),  # Liebauerstr.
        # Tiergarten schotter.
        BoundingBox.box(13.346221, 52.514745, 0.0005, "7"),
        # Prenzlauer Allee.
        BoundingBox.box(13.417535, 52.529313, 0.0005, "8"),
    ]

    dataprovider = DataProvider("data_cache")
    all_rides = dataprovider.get_all_rides()

    for sample in samples:
        ClusterBasedClassifier.bounding_box = sample
        df = ClusterBasedClassifier.classify(
            all_rides, preprocessing_key="new", export_name=sample.title, add_score_hist_data=True)
        if df is not None:
            mean = df.result.mean()
            median = df.result.median()
            count_mean = df['count'].mean()
            std = df.result.std()
            print("title: {}, mean: {}, median: {}, std: {}, mean count: {}".format(
                sample.title, mean, median, std, count_mean))
        else:
            print(None)


def cluster_based():
    test_areas = [BoundingBox.box(13.413215, 52.521918, 0.1, "Alexanderplatz"),
                  BoundingBox.box(13.319665388, 52.507664636,
                                  0.1, "ErnstReuterPlatz"),
                  BoundingBox.box(13.35010, 52.51453, 0.02, "Tiergarten"),
                  BoundingBox.box(13.44172, 52.50120, 0.02, "SchlesischesTor"),
                  BoundingBox.box(13.45401, 52.51574, 0.02, "FrankfurterAllee")]

    dataprovider = DataProvider("data_cache")
    all_rides = dataprovider.get_all_rides()

    for bbox in test_areas:
        ClusterBasedClassifier.bounding_box = bbox
        ClusterBasedClassifier.classify(
            all_rides, preprocessing_key="new", export_name=bbox.title)

    # print(len(dataprovider.get_all_profiles()))
    # print(get_frequencies_for_columns(dataprovider.get_all_rides()))


def signal_processing_based():
    ThresholdBasedClassifier.generate_result("/Users/leonardthomas/simra-surface-analysis/samples", "/Users/leonardthomas/simra-surface-analysis/results")


def main():
    # signal_processing_based()
    evaluate_cluster_based()
    # cluster_based()
    # fire.Fire(CLI)
 #   dataprovider = DataProvider("data_cache")
    #rides = dataprovider.get_all_rides(lambda ride: ride.phone_location == PhoneLocation.HANDLEBAR and ride.duration > 1800)
    #output = "/Users/leonardthomas/simra-surface-analysis/output"
    #CLI().generate_result_for_rides(rides, output)
    #    rides = list(map(lambda x: ThresholdPreprocessing().preprocess(x), dataprovider.get_all_rides()[:1000]))


if __name__ == "__main__":
    main()
