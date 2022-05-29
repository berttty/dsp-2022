import pyspark

@Singleton
class RamuContext:
    """
    It is change to have the context of the execution
    """

    def getSparkContext(self):
        """
        Generate
        :return:
        """
        # TODO: pointing the context to the cluster configuration or
        #       get the configuration using a file
        return pyspark.SparkContext('local[*]')
