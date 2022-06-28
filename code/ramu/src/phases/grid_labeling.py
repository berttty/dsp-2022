import logging
from typing import Callable, Dict

from pyspark import RDD

from context import RamuContext
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
            if len(position) != 5:
                raise Exception("The number of elements is not valid")

            return position[0], float(position[1]), float(position[2]), float(position[3]), float(position[4])

        return convert

    def outputFormatter(self) -> Callable[[Out], str]:
        """
        outputFormatter provide a method to convert content of RDD into text file

        this method need to be implemented
        :return: Callable that will be use as the convertor before to store
        """
        def convert(tuple) -> str:
            return "{} {} {} {} {} {}".format(tuple[0], tuple[1], tuple[2], tuple[3], tuple[4], tuple[5])

        return convert

    def run(self, rdd: RDD[In]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd: the rdd that will use as source
        :return: return the rdd after the elements converted
        """
        def get_label(tu):
            label = randint(0, 100)
            return tu[0], tu[1], tu[2], tu[3], tu[4], label

        return rdd.map(get_label)


def grid_labeling_factory(context: RamuContext, stages: Dict[str, Phase]) -> Phase:
    logging.info('Start factory of GridLabeling')
    current = GridLabeling()
    current.name = 'grid_labeling'
    current.context = context
    current.sink_path = context.get('.stages.grid_labeling.outputs[0].path')
    previous = stages[context.get('.stages.grid_labeling.previous[0].name')]
    if previous is None:
        file_path = context.get('.stages.grid_labeling.inputs[0].path')
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
            print(previous.sink)
            current.source = previous.get_sink()
    logging.info('End factory of GridLabeling')
    return current

