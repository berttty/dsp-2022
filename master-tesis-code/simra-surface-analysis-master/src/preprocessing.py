import pandas as pd
from OSMHelper import *
from simrakit.DataProvider import DataProvider
from simrakit.DataSetStats import *
from simrakit.RideParser import PhoneLocation, Ride

def split(df, column, min_size = 0):
    buckets = []
    previous_index = 0

    for index in df[[column]].dropna().index.tolist():
        integer_location = np.where(df.index == index)[0][0]
        current_sub = df[previous_index:integer_location]

        current_bucket_size = len(current_sub)
        if current_bucket_size > min_size:
            buckets.append(current_sub)

        previous_index = integer_location
    return buckets

def get_features_for_bucket(bucket, origin_ride):
    surface = bucket.surface.dropna().tolist()
    if len(surface) == 0:
        #print("unlabled bucket")
        return None

    label = surface[0]

    def features_for_df(df, prefix):
        df = df[["X","Y","Z"]].describe()
    
        df1=df.stack().swaplevel()
        df1.index=df1.index.map((prefix+'_{0[0]}_{0[1]}').format) 
        df1.to_frame().T
        return df1

    bucket_features = features_for_df(bucket, "b")
    ride_features = features_for_df(origin_ride.ride_df, "r")
    
    result = pd.concat([bucket_features, ride_features], axis=0)
    result["label"] = label
    return result

def main():
    dataprovider = DataProvider("data_cache")

    #load rides
    rides = dataprovider.get_preprocessed_rides()

    if len(rides) == 0:
        print("starting preprocessing")
        
        osmhelper = OSMHelper("/Users/leonardthomas/Downloads/route-annotator-master-2/osm/granderegion.osm.pbf", "/Users/leonardthomas/simra-surface-analysis/data_cache")
        rides = dataprovider.get_all_rides()

        for ride in rides:
            osmhelper.annotate_ride(ride)
            #print(ride.ride_df[["surface"]].dropna())
            dataprovider.save_compressed(ride, ride.title)
        print("finished preprocessing")

    
    feature_row = []
    for ride in rides:
        segments = ride.split_by_interruption()
        for segment in segments:
            #filter out interruptions/ beggining/ end
            clean_segment = segment[3:-3]

            #filter out rides/segments without annotations or that are to fast(cars)
            buckets = split(clean_segment, "surface", 3)

            #generate buckets with feature extraction
            #extract features
            for bucket in buckets:
                data = get_features_for_bucket(bucket, ride)
                if data is not None:
                    feature_row.append(data.to_frame().T)
            
    result = pd.concat(feature_row)
    dataprovider.save_compressed(result, "features", dir="feature_data")
    # randomize samples
    # split intro train & test


if __name__ == "__main__":
    main()
