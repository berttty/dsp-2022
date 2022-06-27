import pyspark

from singleton import singleton


@singleton
class RamuContext:
    """
    It is change to have the context of the execution
    """
    sc = None

    def getSparkContext(self):
        """
        Generate the spark context
        :return:
        """
        # TODO: pointing the context to the cluster configuration or
        #       get the configuration using a file
        if self.sc is None:
            conf = pyspark.SparkConf().setMaster('local[*]').set("spark.hadoop.validateOutputSpecs", "false")
            self.sc = pyspark.SparkContext(conf=conf)

        return self.sc
