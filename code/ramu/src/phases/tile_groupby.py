import json
import logging
from typing import Callable, Dict

import pandas
from pyspark import RDD

from context import RamuContext
from phase import Phase, In, In2, Out
from phase_binary import BiPhase


class TileGroupBy(BiPhase):

    def inputFormatterLeft(self) -> Callable[[str], In]:
        """
        inputFormatter provide a method to convert the text to the type that could be process
        by the Phase

        this method need to be implemented
        :return: Callable that will be use by the map function
        """
        def input_formatter_left(line) -> tuple:
            parts = line.split(" ")
            return parts[0], float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), int(parts[5])

        return input_formatter_left

    def inputFormatterRight(self) -> Callable[[str], In2]:
        """
        inputFormatter provide a method to convert the text to the type that could be process
        by the Phase

        this method need to be implemented
        :return: Callable that will be use by the map function
        """
        def input_formatter_right(line) -> tuple:
            js = json.loads(line)
            li = []
            for x in js['value']:
                li.append(pandas.DataFrame.from_records(x))
            return js['key'], li

        return input_formatter_right

    def outputFormatter(self) -> Callable[[Out], str]:
        """
        outputFormatter provide a method to convert content of RDD into text file

        this method need to be implemented
        :return: Callable that will be use as the convertor before to store
        """
        def output_formatter(tu) -> str:
            return "{} {} {} {} {}".format(
                tu[0],
                type(tu[1][0]),
                tu[1][0],
                type(tu[1][1]),
                len(tu[1][1])
            )

        return output_formatter

    def run(self, rdd_left: RDD[In], rdd_right: RDD[In2]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd_left: the rdd that will use as source for the left side of the binary operator
        :param rdd_right: the rdd that will use as source for the right side of the binary operator
        :return: return the rdd after the elements converted
        """
        return rdd_left.join(rdd_right)


def tile_group_by_factory(context: RamuContext, stages: Dict[str, Phase]) -> Phase:
    logging.info('Start factory of TileGroupBy')
    current = TileGroupBy()
    current.name = 'tile_group_by'
    current.context = context
    current.sink_path = context.get('.stages.tile_group_by.outputs[0]')
    previous_left = stages[context.get('.stages.tile_group_by.previous[0].name')]
    previous_right = stages[context.get('.stages.tile_group_by.previous[1].name')]
    if previous_left is None:
        file_path = context.get('.stages.tile_group_by.inputs[0]')
        logging.info('The stage "%s" will use the file "%s" as source', current.name, file_path)

        current.source_path_left = file_path
    else:
        if previous_left.get_sink() is None:
            logging.info(
                'The stage "%s" will use the file "%s" as source coming from the stage "%s"',
                current.name,
                previous_left.sink_path,
                previous_left.name
            )
            current.source_path_left = previous_left.sink_path
        else:
            logging.info(
                'The stage "%s" will use the output from the stage "%s" directly',
                current.name,
                previous_left.name
            )
            current.source_left = previous_left.get_sink()

    if previous_right is None:
        file_path = context.get('.stages.tile_group_by.inputs[1]')
        logging.info('The stage "%s" will use the file "%s" as source', current.name, file_path)
        current.source_path_right = file_path
    else:
        if previous_right.get_sink() is None:
            logging.info(
                'The stage "%s" will use the file "%s" as source coming from the stage "%s"',
                current.name,
                previous_right.sink_path,
                previous_right.name
            )
            current.source_path_right = previous_right.sink_path
        else:
            logging.info(
                'The stage "%s" will use the output from the stage "%s" directly',
                current.name,
                previous_right.name
            )
            current.source_right = previous_right.get_sink()

    logging.info('End factory of TileGroupBy')
    return current
