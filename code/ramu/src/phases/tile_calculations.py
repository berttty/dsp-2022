import json
import math
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

        def input_formatter(line: str) -> tuple:
            js = json.loads(line)
            return js['key'], pandas.DataFrame.from_records(js['value'])

        return input_formatter

    def outputFormatter(self) -> Callable[[Out], str]:
        """
        outputFormatter provide a method to convert content of RDD into text file

        this method need to be implemented
        :return: Callable that will be use as the convertor before to store
        """

        def output_formatter(t: tuple) -> str:
            #value = {'key': t[0], 'value': t[1].to_dict('records')}
            l = []
            for x in t[1]:
                l.append(x.to_dict('records'))
            value = {'key': t[0], 'value': l}
            return json.dumps(value)

        return output_formatter

    def run(self, rdd: RDD[In]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd: the rdd that will use as source
        :return: return the rdd after the elements converted
        """

        def extract_key(element):
            return len(element)

        def calculate_usage(origin):
            elements = list(origin)
            size: int = len(elements)
            if size < 3:
                return elements
            #TODO: retrieve reduction from configuration file
            reduction: float = 0.10
            cut: int = math.ceil(size * reduction)
            return sorted(elements, key=extract_key)[cut:-cut]

        return rdd.groupByKey().mapValues(calculate_usage)
