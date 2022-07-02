import json
import logging

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
        logging.info('the RamuContext will be created using the file "%s"', conf_file_path)
        with open(conf_file_path, 'r') as myfile:
            data = myfile.read()

        self.configuration_json = json.loads(data)

    def get_spark(self):
        """
        Generate the spark context
        :return:
        """
        if self.sc is None:
            conf = pyspark.SparkConf()
            master = self.get('.conf.spark.master', 'local[*]')
            logging.info('the spark will use as master: "%s"', master)
            conf = conf.setMaster(master)

            others = self.get('.conf.spark.others', {})
            logging.info('the spark others configuration are: %s', others)
            for key, value in others.items():
                conf = conf.set(key, value)

            self.sc = pyspark.SparkContext(conf=conf)

        return self.sc

    def get(self, query: str, default_value=None):
        result = jq.compile(query).input(self.configuration_json).first()
        if result is None:
            logging.warning('the variable "%s" will use the default value "%s"', query, default_value)
            result = default_value
        if type(result) is dict:
            if 'type' in result and 'value' in result:
                if result['type'] == 'key':
                    return self.get(result['value']);
                else:
                    return result['value']
        return result


