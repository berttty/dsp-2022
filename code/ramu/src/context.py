import json

import jq
import pyspark

from singleton import singleton


@singleton
class RamuContext:
    """
    It is change to have the context of the execution
    """
    sc = None
    configuration_json = None

    def __init__(self, conf_file_path: str = 'configuration.json'):
        # read file
        with open(conf_file_path, 'r') as myfile:
            data = myfile.read()

        self.configuration_json = json.loads(data)


    def getSparkContext(self):
        """
        Generate the spark context
        :return:
        """
        # TODO: pointing the context to the cluster configuration or
        #       get the configuration using a file
        if self.sc is None:
            conf = pyspark.SparkConf().setMaster('local[*]').set("spark.hadoop.validateOutputSpecs", "false").set('spark.executor.memory','4g')
            self.sc = pyspark.SparkContext(conf=conf)

        return self.sc

    def get(self, query: str):
        return jq.compile(query).input(self.configuration_json).first()


