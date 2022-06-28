import json
import logging
from typing import Callable, Dict

import pandas
from pyarrow._dataset import Dataset
from pyspark import RDD

from context import RamuContext
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

        cities = self.context.get('.conf.cities')
        DICT = {}
        for city in cities:
            DICT[city] = get_rules(city)

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


def tile_generation_factory(context: RamuContext, stages: Dict[str, Phase]) -> Phase:
    logging.info('Start factory of TileGeneration')
    current = TileGeneration()
    current.name = 'tile_generation'
    current.context = context
    current.sink_path = context.get('.stages.tile_generation.outputs[0]')
    previous = stages[context.get('.stages.tile_generation.previous[0].name')]
    if previous is None:
        file_path = context.get('.stages.tile_generation.inputs[0]')
        logging.info('The stage "%s" will use the file "%s" as source', current.name, file_path)
        current.source_path = file_path
    else:
        if previous.get_sink() is None:
            logging.info(
                'The stage "%s" will use the file "%s" as source coming from the stage "%s"',
                current.name,
                previous.sink_path,
                previous.name
            )
            current.source_path = previous.sink_path
        else:
            logging.info(
                'The stage "%s" will use the output from the stage "%s" directly',
                current.name,
                previous.name
            )
            current.source = previous.get_sink()
    logging.info('End factory of TileGeneration')
    return current
