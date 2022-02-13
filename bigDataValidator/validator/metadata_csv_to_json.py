import csv
from dataclasses import replace
import json
import os

class MetadataCsvToJson:

    def createDictFromCsv(self,path_to_csv):
        with open(path_to_csv) as metadata:
            metadata_reader = csv.reader(metadata)
            jsonDict = dict()
            jsonDict["TypeMapping"] = list()
            counter = 0
            for row in metadata_reader:
                values = row[0].split(";")
                if counter == 0:
                    counter = counter+1
                    continue
                elif counter == 1:
                    jsonDict["DecimalSeparator"] = values[4]
                    jsonDict["FieldSeparator"] = values[3]
                    jsonDict["StringSeparator"] = values[2]
                    jsonDict["TypeMapping"].append(self.getColumnDict(values))
                else:
                    jsonDict["TypeMapping"].append(self.getColumnDict(values))

                counter = counter+1
            return jsonDict

    def getColumnDict(self,values):
        column_dict = dict()

        column_dict["ColumnName"] = values[0]
        column_dict["SourceDataType"] = values[1]
        column_dict["SourceDataFormat"] = values[6]
        column_dict["SourceNullable"] = values[5]

        return column_dict
        
    def convertCsvToJson(self,path_to_csv):
        meta_dict = self.createDictFromCsv(path_to_csv)
        path_to_json = path_to_csv.replace("csv","json")
        with open(path_to_json, mode="w") as output:
            json.dump(meta_dict,output,indent=4)

