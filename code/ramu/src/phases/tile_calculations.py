import json
from typing import Callable

import pandas
from pyarrow._dataset import Dataset
from pyspark import RDD
from phase import Phase, In, Out


class TileUsageCalculation(Phase):

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

