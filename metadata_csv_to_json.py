import csv
import json

class MetadataCsvToJson:

    def __init__(self):
        #Maybe initialize path based on arguments

    def readCsvFile(self,path_to_csv):
        with open(path_to_csv,mode="r") as metadata:
            metadata_reader = csv.reader(metadata)
            for row in metadata_reader:
                print(row)