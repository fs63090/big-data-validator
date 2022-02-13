from abc import abstractmethod
import sys
import json

from metadata_csv_to_json import MetadataCsvToJson
from pyspark.sql import SparkSession

class BaseDataValidation:

    def __init__(self):
        self.argument = self.get_config()
        self.sc = None
        self.spark = None
        self.logger = None
        self.actual_columns = None
        self.corruptcol_name = "CorruptRecCol"

    def setup_sc(self):
        self.spark = SparkSession.builder.appName('ETL').getOrCreate()
        self.sc = self.spark._sc

    def setup_log(self):
        log4j = self.sc._jvm.org.apache.log4j
        message_prefix = '<DATA-VALIDATOR>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def get_config(self):
        try:
            print(sys.argv[1])
            return sys.argv[1]
        except IndexError:
            print("No configuration was passed!")
            raise IndexError

    @abstractmethod
    def read_input_table(self,metadata,tableName):
        pass 
    

    def read_metadata_json(self,tableName):
        path_to_metadata = "metadata/csv/"+tableName+"_metadata.csv"
        MetadataCsvToJson().convertCsvToJson(path_to_metadata)
        try:
            with open(path_to_metadata.replace("csv","json"), mode="r") as reader:
                return json.loads(reader.read())
        except IndexError:
            print("No configuration was passed!")
            raise IndexError

    @abstractmethod
    def validation_main(self,metadata):
        pass

    def main(self):
        self.setup_sc()
        self.setup_log()
        metadata = self.read_metadata_json(self.argument)
        test = self.validation_main(metadata)