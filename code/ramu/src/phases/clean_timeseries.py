import logging
from typing import Callable, Dict

import pandas
import io
from pyspark import RDD

from context import RamuContext
from phase import Phase, In, Out

from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import acf
import numpy as np


# main decomposition function
# takes a dataframe and calculates the decompostion for every axis
#
def decompose(df):
    result_list = {}
    for direction in ["X", "Y", "Z"]:
        dp = df[direction]
        # we need to find the lag with highest correlation to use in sesonal decompostion
        # in my understandig the lag (with highest corr) will define the period
        all_auto = acf(dp, nlags=len(dp) // 2, alpha=None, fft=True)
        # ac[2:] for cutting lag 0 with r-value of 1
        # and lag 1 as it cannot be used with seasonel_decompose effectively
        max_corr_lag = np.abs(all_auto[2:]).argmax() + 2

        # as a alternative we need to find a fix value?!

        result_add = seasonal_decompose(dp, model='additive', extrapolate_trend='freq', period=max_corr_lag)
        result_list[direction] = {"observed": result_add.observed,
                                  "seasonal": result_add.seasonal,
                                  "trend": result_add.trend,
                                  "resid": result_add.resid}
    return result_list

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
            if pd.shape[0] <= 10:
                logging.warning(f'the file "{tuple[0]}" was dropped because it contains {pd.shape[0]} rows, and they are not enough to analyse it')
                return None

            if 'lat' not in pd.columns:
                pd = pd.rename(columns={'at': 'lat'})

            pd = pd[['lat', 'lon', 'X', 'Y', 'Z', 'timeStamp']]

            try:
                decomposed = decompose(pd)
            except Exception as e:
                print(tuple[0])
                raise e

            for direction in decomposed:
                elem = decomposed[direction]
                for kind in elem:
                    key = f'{direction}_{kind}'
                    pd[key] = elem[kind]

            return pd

        def is_none(pd):
            return pd is not None

        return rdd.map(convert).filter(is_none)


def clean_timeseries_factory(context: RamuContext, stages: Dict[str, Phase]) -> Phase:
    logging.info('Start factory of CleanTimeSeries')
    current = CleanTimeSeries()
    current.name = 'clean_timeseries'
    current.context = context
    current.sink_path = context.get('.stages.clean_timeseries.outputs[0]')
    current.source_path = context.get('.stages.clean_timeseries.inputs[0]')
    partitions: int = int(context.get('.stages.clean_timeseries.conf.repartition'))
    logging.info(f'Stage CleanTimeSeries will use {partitions} partitions')
    current.source = context.get_spark()\
                            .wholeTextFiles(current.source_path)\
                            .repartition(partitions)
    logging.info('End factory of CleanTimeSeries')
    return current
