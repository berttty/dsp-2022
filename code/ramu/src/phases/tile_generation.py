import json
from typing import Callable

import pandas
from pyarrow._dataset import Dataset
from pyspark import RDD
from phase import Phase, In, Out
from phases.grid_generation import get_identifier, get_rules


class TileGeneration(Phase):

    def inputFormatter(self) -> Callable[[str], In]:
        """
        inputFormatter provide a method to convert the text to the type that could be process
        by the Phase

        this method need to be implemented
        :return: Callable that will be use by the map function
        """
        def input_formatter(line: str) -> Dataset:
            js = json.loads(line)
            return pandas.DataFrame.from_records(js)

        return input_formatter

    def outputFormatter(self) -> Callable[[Out], str]:
        """
        outputFormatter provide a method to convert content of RDD into text file

        this method need to be implemented
        :return: Callable that will be use as the convertor before to store
        """
        def output_formatter(t: tuple) -> str:
            value = {'key': t[0], 'value': t[1].to_dict('records')}
            return json.dumps(value)

        return output_formatter

    def run(self, rdd: RDD[In]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd: the rdd that will use as source
        :return: return the rdd after the elements converted
        """
# TODO validate how to get parametrical
        DICT = {}
        DICT['berlin'] = get_rules('berlin')

        def calculate_identifier(city: str, latitude, longitude):
            latitudes, longitudes = DICT[city]
            index_lat = -1
            index_lon = -1
            for index, item in enumerate(latitudes, start=0):
                if index == 0:
                    continue
                if item <= latitude < latitudes[index - 1]:
                    index_lat = index
                    break
            for index, item in enumerate(longitudes, start=0):
                if index == 0:
                    continue
                if item >= longitude > longitudes[index - 1]:
                    index_lon = index
                    break

            return get_identifier(city, index_lat, index_lon, len(latitudes))

        def func(pd):
            curr_lat = 0
            curr_lon = 0
            identifier = None
            start = 0
            if 'lat' not in pd.columns:
                print(pd.head)
                return
            if 'lon' not in pd.columns:
                print(pd.head)
                return

            for index, row in pd.iterrows():
                if curr_lon != row['lon'] or curr_lat != row['lat']:
                    curr_lon = row['lon']
                    curr_lat = row['lat']
                    pre_identifier = identifier
                    #TODO validate the name with values
                    identifier = calculate_identifier('berlin', curr_lat, curr_lon)
                    if pre_identifier != None and pre_identifier != identifier:
                        yield identifier, pd.iloc[start:index]
                        start = index

            yield identifier, pd.iloc[start:]

        return rdd.flatMap(func)
