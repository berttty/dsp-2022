from typing import Callable

from pyspark import RDD

from phase import Phase, In, Out

import geopy.distance
from variable import CITY


OPT_DIRECTION = {'north': 0, 'south': 180, 'east': 90, 'west': -90}


def nextpoint(latitude, longitude, direction: str, size: int = 100):
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


def grid(city: str, size: int = 100):
    """
    Grid generator for the giving city, and following size given in meters
    :param city: name of the city where the grid will be created
    :param size: size in meters of the tile generated
    :return: double pair coodinates, that indicated the start and end of tails
    """
    latitude_start = CITY[city]['start']['latitude']
    longitude_start = CITY[city]['start']['longitude']
    latitude_end = CITY[city]['end']['latitude']
    longitude_end = CITY[city]['end']['longitude']

    latitude_pre = latitude_start
    latitude, _ = nextpoint(latitude_start, longitude_start, 'south', size)

    while latitude > latitude_end:
        longitude = longitude_start
        while longitude < longitude_end:
            _, longitude_next = nextpoint(latitude, longitude, 'east', size)
            yield latitude_pre, longitude, latitude, longitude_next
            longitude = longitude_next
        latitude_pre = latitude
        latitude, _ = nextpoint(latitude_pre, longitude_start, 'south', size)


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
            return "{} {} {} {}".format(tuple[0], tuple[1], tuple[2], tuple[3])

        return convert

    def run(self, rdd: RDD[In]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd: the rdd that will use as source
        :return: return the rdd after the elements converted
        """
        return rdd.flatMap(lambda city: grid(city)).repartition(12)



