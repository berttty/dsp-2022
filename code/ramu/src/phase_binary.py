import logging
from typing import Callable

from pyspark import RDD

from phase import Phase, In, In2, Out


class BiPhase(Phase):
    """
    Phase is the structure that encapsulate one step inside of the workflow, but also allows
    """
    source_path_left: str = None
    source_left: RDD[In] = None
    source_path_right: str = None
    source_right: RDD[In2] = None

    def inputFormatterLeft(self) -> Callable[[str], In]:
        """
        inputFormatter provide a method to convert the text to the type that could be process
        by the Phase

        this method need to be implemented
        :return: Callable that will be use by the map function
        """
        return None

    def inputFormatterRight(self) -> Callable[[str], In2]:
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

    def getRDDSourceLeft(self) -> RDD[In]:
        """
        get or create the source for the Phase

        :return: RDD instance that is use as source
        """
        if self.source_left is not None:
            return self.source_left

        if self.source_path_left is None:
            raise Exception("the source_path could not be None")

        # TODO validate the source_path have the correct format

        converter = self.inputFormatterLeft()
        if converter is None:
            raise Exception(
                "the inputFormatter is not defined correctly, please check the implementation of {}".format(
                    type(self).__name__
                )
            )

        logging.info("The stage '%s' will use a file '%s' as source RDD", self.name, self.source_path_left)
        sc = self.context.get_spark()
        return sc.textFile(self.source_path_left) \
            .map(converter)

    def getRDDSourceRight(self) -> RDD[In]:
        """
        get or create the source for the Phase

        :return: RDD instance that is use as source
        """
        if self.source_right is not None:
            return self.source_right

        if self.source_path_right is None:
            raise Exception("the source_path could not be None")

        # TODO validate the source_path have the correct format

        converter = self.inputFormatterRight()
        if converter is None:
            raise Exception(
                "the inputFormatter is not defined correctly, please check the implementation of {}".format(
                    type(self).__name__
                )
            )

        logging.info("The stage '%s' will use a file '%s' as source RDD", self.name, self.source_path_right)
        sc = self.context.get_spark()
        return sc.textFile(self.source_path_right) \
            .map(converter)

    def run(self, rdd_left: RDD[In], rdd_right: RDD[In2]) -> RDD[Out]:
        """
        run is the method that contains the logic of the phase
        :param rdd_left: the rdd that will use as source for the left side of the binary operator
        :param rdd_right: the rdd that will use as source for the right side of the binary operator
        :return: return the rdd after the elements converted
        """

    def execute(self):
        """
        execute the pipeline of the current phase
        :return:
        """
        logging.info("the stage '%s' is Executing", self.name)
        rdd_left = self.getRDDSourceLeft()
        rdd_right = self.getRDDSourceRight()
        processed: RDD[Out] = self.run(rdd_left, rdd_right)

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
