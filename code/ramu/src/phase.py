import logging

from pyspark import RDD

from context import RamuContext
from typing import (Callable, TypeVar)

In = TypeVar("In")
Out = TypeVar("Out")


class Phase:
    """
    Phase is the structure that encapsulate one step inside of the workflow, but also allows
    """
    context: RamuContext = None
    name: str = None
    source_path: str = None
    source: RDD[In] = None

    sink_path: str = None
    sink: RDD[Out] = None

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
        return None

    def getRDDSource(self) -> RDD[In]:
        """
        get or create the source for the Phase

        :return: RDD instance that is use as source
        """
        if self.source is not None:
            return self.source

        if self.source_path is None:
            raise Exception("the source_path could not be None")

        # TODO validate the source_path have the correct format

        converter = self.inputFormatter()
        if converter is None:
            raise Exception(
                "the inputFormatter is not defined correctly, please check the implementation of {}".format(
                    type(self).__name__
                )
            )

        logging.info("The stage '%s' will use a file '%s' as source RDD", self.name, self.source_path)
        sc = self.context.get_spark()
        return sc.textFile(self.source_path) \
                 .map(converter)

    def run(self, rdd: RDD[In]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd: the rdd that will use as source
        :return: return the rdd after the elements converted
        """

    def store(self, rdd: RDD[Out]):
        """
        store create a file and save the information using the methods
        :param rdd: it take the
        :return:
        """
        if self.sink_path is None:
            raise Exception("the sink_path could not be None")

        # TODO validate the source_path have the correct format

        converter = self.outputFormatter()
        if converter is None:
            raise Exception(
                "the outputFormatter is not defined correctly, please check the implementation of {}".format(
                    type(self).__name__
                )
            )

        rdd.map(converter)\
           .saveAsTextFile(self.sink_path)

    def get_sink(self) -> RDD[Out]:
        """
        obtain the sink of the phase this could be use by other phase
        :return: return the sink of the current phase
        """
        return self.sink

    def execute(self):
        """
        execute the pipeline of the current phase
        :return:
        """
        logging.info("the stage '%s' is Executing", self.name)
        rdd = self.getRDDSource()
        processed: RDD[Out] = self.run(rdd)
        if self.sink_path is None:
            self.sink = processed
            return

        logging.info("the stage '%s' will be saved on the file '%s'", self.name, self.sink_path)

        is_cached = self.context.get(
            '.stages.{}.conf.cache'.format(self.name),
            False
        )
        if is_cached:
            logging.info("the stage '%s' RDD is cached", self.name)
            self.sink = processed.cache()
        else:
            logging.info("the stage '%s' RDD is NOT cached", self.name)
            self.sink = processed

        self.store(self.sink)
