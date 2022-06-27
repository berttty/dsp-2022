from typing import Callable

import pandas
import io
from pyspark import RDD
from phase import Phase, In, Out

class CleanTimeSeries(Phase):

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
        outputFormatter provide a method to convert content of RDD into text file

        this method need to be implemented
        :return: Callable that will be use as the convertor before to store
        """

        def convert(pd) -> str:
            return str(pd.to_json(orient='records'))

        return convert

    def run(self, rdd: RDD[In]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd: the rdd that will use as source
        :return: return the rdd after the elements converted
        """
        def convert(tuple):
            position = tuple[1].index('=\n') + 2
            for character in tuple[1]:
                position = position + 1
                if character == '\n':
                    break

            return pandas.read_csv(io.StringIO(tuple[1][position:]), sep=",").ffill()

        return rdd.map(convert)
