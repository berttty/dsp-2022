import logging
from typing import Callable, Dict

import pandas
import io
from pyspark import RDD

from context import RamuContext
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
            try:
                return str(pd.to_json(orient='records'))
            except Exception as ex:
                print(pd)
                raise Exception(ex)

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

            pd = pandas.read_csv(io.StringIO(tuple[1][position:]), sep=",").ffill()
            if 'lat' not in pd.columns:
                pd = pd.rename(columns={'at': 'lat'})
            return pd

        return rdd.map(convert)


def clean_timeseries_factory(context: RamuContext, stages: Dict[str, Phase]) -> Phase:
    logging.info('Start factory of CleanTimeSeries')
    current = CleanTimeSeries()
    current.name = 'clean_timeseries'
    current.context = context
    current.sink_path = context.get('.stages.clean_timeseries.outputs[0].path')
    current.source_path = context.get('.stages.clean_timeseries.inputs[0].path')
    partitions: int = int(context.get('.stages.clean_timeseries.conf.repartition'))
    current.source = context.get_spark()\
                            .wholeTextFiles(current.source_path)\
                            .repartition(partitions)
    logging.info('End factory of CleanTimeSeries')
    return current
