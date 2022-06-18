import pyspark

from singleton import singleton


@singleton
class RamuContext:
    """
    It is change to have the context of the execution
    """

    def getSparkContext(self):
        """
        Generate the spark context
        :return:
        """
        # TODO: pointing the context to the cluster configuration or
        #       get the configuration using a file
        return pyspark.SparkContext('local[*]')
