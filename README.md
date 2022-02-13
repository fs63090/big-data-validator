This repo consists of the following:

INPUTS directory:
    This is the directory where all input files will automatically be added to, with the naming convention:
        <TableName>.csv

LOGGER directory:
    Contains the log4j properties file, which is necessary for logging during execution

METADATA directory:
    Contains two subdirectories:
        CSV:
            This is the directory where all metadata csv files will be automatically added to, with the naming convention:
                <TableName>_metadata.csv
        JSON:
            This is the directory where all metadata json files, used as intermediary will be automatically added to, 
            with the naming convention: <TableName>_metadata.json

There are also 3 python classes

1. metadata_csv_to_json:
    Contains the MetadataCsvToJson class, and three methods, used to generate the JSON metadata from the CSV metadata.

2. base_data_validation.py:
    This is the base class for the third one, which is used for arguments parsing, logger and spark setup, as well as preparing 
    the metadata and calling the validation function - implemented in the third class.

3. big_data_validator.py:
    This is the class with the main functionality, extending the BaseDataValidation class, and implementing its abstract 
    methods.
    
To execute this from the CMD, change directory to this folder and execute the following command:
spark-submit --deploy-mode client --driver-java-options -Dlog4j.configuration=file:logger/log4j.properties big_data_validator.py <TABLE_NAME>


The TABLE_NAME should only be for example: LMIS_APPLICATION