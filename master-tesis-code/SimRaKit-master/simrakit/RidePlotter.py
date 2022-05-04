import geopandas
import pandas as pd
from shapely.geometry import Point, LineString, shape
from ipyleaflet import *

class RidePlotter(object):

    def __init__(self, ride):
        self.ride = ride

    # TODO: add suppoer for sample_count
    def display_on_map(self):    
        df = self.ride.ride_df
        df.timeStamp = pd.to_numeric(df.timeStamp, errors = 'coerce',downcast='integer')

        gdf = geopandas.GeoDataFrame(df.reset_index(drop=True), geometry=geopandas.points_from_xy(df.lon, df.lat)).dropna()
        gdf["geometry"] = LineString(gdf['geometry'].tolist())
        gdf = geopandas.GeoDataFrame(gdf, geometry='geometry')

        ride_data = GeoData(geo_dataframe = gdf,
                            style={},
                            hover_style={},
                            name = 'Locations')


        m = Map(center=(gdf.centroid.iloc[0].y, gdf.centroid.iloc[0].x), zoom=10)

        m.add_layer(ride_data)
        return m

#TODO Simple plotting functions to plot different properties, location, accelerometer