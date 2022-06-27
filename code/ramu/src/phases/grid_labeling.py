from typing import Callable

from pyspark import RDD
from phase import Phase, In, Out
from random import randint


class GridLabeling(Phase):

    def inputFormatter(self) -> Callable[[str], In]:
        """
        inputFormatter provide a method to convert the text to the type that could be process
        by the Phase

        this method need to be implemented
        :return: Callable that will be use by the map function
        """
        def convert(line) -> tuple[float, float, float, float]:
            position = line.split(" ")
            if len(position) != 4:
                raise Exception("The number of elements is not valid")

            return float(position[0]), float(position[1]), float(position[2]), float(position[3])

        return convert

    def outputFormatter(self) -> Callable[[Out], str]:
        """
        outputFormatter provide a method to convert content of RDD into text file

        this method need to be implemented
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
        def get_label(tuple):
            label = randint(0, 100)
            return tuple[0], tuple[1], tuple[2], tuple[3], label

        return rdd.map(get_label)

