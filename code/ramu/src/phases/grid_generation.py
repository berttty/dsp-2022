import math
from typing import Callable, Dict

from pyspark import RDD

from context import RamuContext
from phase import Phase, In, Out

import geopy.distance
from variable import CITY


OPT_DIRECTION = {'north': 0, 'south': 180, 'east': 90, 'west': -90}


def calculate_number_tiles(latitude_start, longitude_start, latitude_end, longitude_end, size) -> int:
    return 1 + math.ceil(
        geopy.distance.geodesic(
            (latitude_start, longitude_start),
            (latitude_end, longitude_end)
        ).meters / size
    )


def generate_ruler_longitude(longitude, latitude_start, latitude_end, size: int = 100):
    tiles: int = calculate_number_tiles(latitude_start, longitude, latitude_end, longitude, size)
    latitudes = [None] * tiles
    index = 0
    latitude = latitude_start
    latitudes[index] = latitude
    while latitude > latitude_end:
        latitude, _ = next_point(latitude, longitude, 'south', size)
        index = index + 1
        latitudes[index] = latitude

    return latitudes


def generate_ruler_latitude(latitude, longitude_start, longitude_end, size: int = 100):
    tiles: int = calculate_number_tiles(latitude, longitude_start, latitude, longitude_end, size)
    longitudes = [None] * tiles
    index = 0
    longitude = longitude_start
    longitudes[index] = longitude
    while longitude < longitude_end:
        _, longitude = next_point(latitude, longitude, 'east', size)
        index = index + 1
        longitudes[index] = longitude

    return longitudes


def next_point(latitude, longitude, direction: str, size: int = 100):
    """
    Calculate the next point from the point(latitude, longitude) in distance of 'size' in the direction
    'north', 'south', 'east', 'west'
    :param latitude: latitude of starting point
    :param longitude: longitude of starting point
    :param direction: direction for the next point
    :param size: size of the movement
    :return: coordinates pair at distance equal to 'size' in the direction requested
    """
    bearing = OPT_DIRECTION[direction]
    point = geopy.distance.distance(meters=size).destination((latitude, longitude), bearing=bearing)
    return point.latitude, point.longitude


def get_rules(city: str, size: int = 100):
    latitude_start = CITY[city]['start']['latitude']
    longitude_start = CITY[city]['start']['longitude']
    latitude_end = CITY[city]['end']['latitude']
    longitude_end = CITY[city]['end']['longitude']

    longitudes = generate_ruler_latitude(latitude_start, longitude_start, longitude_end, size)
    latitudes = generate_ruler_longitude(longitude_start, latitude_start, latitude_end, size)

    return latitudes, longitudes


def get_identifier(city: str, index_lat, index_lon, n_lat):
    return '{}_{}'.format(city, (((n_lat - 1) * (index_lon - 1)) + (index_lat - 1)))


def grid(city: str, size: int = 100):
    """
    Grid generator for the giving city, and following size given in meters
    :param city: name of the city where the grid will be created
    :param size: size in meters of the tile generated
    :return: double pair coodinates, that indicated the start and end of tails
    """
    latitudes, longitudes = get_rules(city, size)

    n_latitudes = len(latitudes)
    n_longitudes = len(longitudes)

    for index_lon in range(1, n_longitudes):
        for index_lat in range(1, n_latitudes):
            yield \
                get_identifier(city, index_lat, index_lon, n_latitudes), \
                latitudes[index_lat - 1], \
                longitudes[index_lon - 1], \
                latitudes[index_lat], \
                longitudes[index_lon]


class GridGeneration(Phase):

    def inputFormatter(self) -> Callable[[str], In]:
        """
        inputFormatter provide a method to convert the text to the type that could be process
        by the Phase

        this method need to be implemented
        :return: Callable that will be use by the map function
        """
        return None

    def outputFormatter(self) -> Callable[[Out], str]:
        """
        convert the (latitude_start, longitude_start, latitude_end, longitude_end) to a string
        :return: Callable that will be use as the convertor before to store
        """
        def convert(tuple) -> str:
            return "{} {} {} {} {}".format(tuple[0], tuple[1], tuple[2], tuple[3], tuple[4])

        return convert

    def run(self, rdd: RDD[In]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd: the rdd that will use as source
        :return: return the rdd after the elements converted
        """

        return rdd\
            .flatMap(lambda city: grid(city))\
            .repartition(
                self.context.get('.stages.grid_generation.conf.repartition')
            )


def grid_generation_factory(context: RamuContext, stages: Dict[str, Phase]) -> Phase:
    current = GridGeneration()
    current.context = context
    current.sink_path = context.get('.stages.grid_generation.outputs[0].path')
    current.source = context.getSparkContext().parallelize(context.get('.stages.grid_generation.inputs[0].list'))
    return current
