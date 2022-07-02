import json
import logging
import math
from typing import Callable, Dict

import pandas
from pyspark import RDD

from context import RamuContext
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
            # value = {'key': t[0], 'value': t[1].to_dict('records')}
            li = []
            for x in t[1]:
                li.append(x.to_dict('records'))
            value = {'key': t[0], 'value': li}
            return json.dumps(value)

        return output_formatter

    def run(self, rdd: RDD[In]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd: the rdd that will use as source
        :return: return the rdd after the elements converted
        """

        reduction: float = self.context.get('.stages.tile_usage_calculation.conf.reduction')

        def extract_key(element):
            return len(element)

        def calculate_usage(origin):
            elements = list(origin)
            size: int = len(elements)
            if size < 3:
                return elements
            cut: int = math.ceil(size * reduction)
            return sorted(elements, key=extract_key)[cut:-cut]

        return rdd.groupByKey().mapValues(calculate_usage)


def tile_usage_calculation_factory(context: RamuContext, stages: Dict[str, Phase]) -> Phase:
    logging.info('Start factory of TileUsageCalculation')
    current = TileUsageCalculation()
    current.name = 'tile_usage_calculation'
    current.context = context
    current.sink_path = context.get('.stages.tile_usage_calculation.outputs[0]')
    previous = stages[context.get('.stages.tile_usage_calculation.previous[0].name')]
    if previous is None:
        file_path = context.get('.stages.tile_usage_calculation.inputs[0]')
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
    logging.info('End factory of TileUsageCalculation')
    return current
