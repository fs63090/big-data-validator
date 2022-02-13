import json
import re
import sys

from pyspark.sql.types import (StringType, StructField, StructType)
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import BaseDataValidation

#BaseDataValidation abstract class:
#abstract method: validate(self)
#implementon metode qe osht gjenerale per krejt validimet.


#Klase e re:
#csvToJsonConverter

class IngestionValidation(BaseDataValidation):

    def __init__(self):
        self.conf = self.get_config()
        self.sc = None
        self.spark = None
        #self.s3 = self.get_s3()
        self.logger = None
        self.actual_columns = None
        self.corruptcol_name = "CorruptRecCol"
        self.interchange_file = {}

    @staticmethod
    def get_config():
        """
        load configuration from sys.argv in json format (Not json file!)
        :return: config dict
        """
        try:
            # with open(sys.argv[1]) as f:
            #     return json.loads(f.read())
            return json.loads(sys.argv[1])
        except IndexError:
            print("No configuration was passed!")
            raise IndexError

    '''def get_input_paths(self):
        # TODO clarify!
        file_list = json.loads(self.s3.Object(
            self.conf['Interchange'],
            self.conf['InterchangeFilePath'],
        ).get()['Body'].read().decode('utf-8'))
        return (
            f['key']
            for f in file_list
            # TODO Maybe some better idea to detect which table do we work on
            if f['key'].split('/')[4].upper() == self.table_name.upper()
        )'''


    @staticmethod
    def get_string_separator(metadata):
        # type: (dict) -> str
        quotes = metadata['StringSeparator']
        if str(quotes) == 'nan':
            quotes = '\\'
        return quotes

    @staticmethod
    def get_field_separator(metadata):
        # type: (dict) -> str
        delimiter = metadata['FieldSeparator']
        return delimiter

    @property
    def table_name(self):
        # type: () -> str
        """Get configured table name for this validation"""
        return self.conf['TableName']

    @property
    def interchange_bucket(self):
        # type: () -> str
        """Get interchange bucket name"""
        return self.conf['Interchange']

    @property
    def metadata_object(self):
        """Get metadata object as boto S3 Object"""
        #Metadata lexohet tash lokalisht, check mos osht JSON file (intermediary)
        #New class idea (csvToJsonConverter)
        return self.s3.Object(
            self.interchange_bucket,
            self.conf['MetadataJSONS3Path'],
        )

    def setup_sc(self):
        self.spark = SparkSession.builder.appName('ETL').getOrCreate()
        self.sc = self.spark._sc

    def setup_log(self):
        log4j = self.sc._jvm.org.apache.log4j
        message_prefix = '<DATA-VALIDATOR>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def get_table_metadata(self):
        """
        get Json metadata (provided by the data prodivider)
        :return:
        """
        interchange_bucket = self.conf['Interchange']
        metadata_json = self.conf['MetadataJSONS3Path']
        metadata_json = json.loads(
            self.s3.Object(interchange_bucket, metadata_json).get()['Body'].read().decode('utf-8'))  # type: dict

        try:
            return next(
                table_dict for table_dict in metadata_json if table_dict['TableName'] == self.table_name
            )
        except StopIteration:
            raise ValueError(
                'Table {} not found in metadata JSON'.format(self.table_name))

    def validate_column_names(self, rdd, metadata):
        """
        In this validation we would like to check if the column names in the header of the data match
         with the column names from the metadata
         Method:
         compare the two lists and get the differences.
        :param rdd: input file
        :param metadata: data descriptor from data provider
        :return: If there is no difference return True, False otherwise.
        """
        self.logger.info("validate column names")
        # TODO Consider having multiple files with mismatching schema.
        #      In this case the validation will not work as intended
        self.actual_columns = [
            (
                element.strip(metadata['StringSeparator'])
                if 'StringSeparator' in metadata
                else element
            ).upper()
            for element
            in rdd.first().split(metadata['FieldSeparator'])
        ]
        expected_columns = [
            c['ColumnName'].upper()
            for c in metadata['TypeMapping']
        ]

        if set(self.actual_columns).issuperset(expected_columns):
            self.logger.info(
                '\n'.join([
                    'Column Names Validation PASSED!',
                    'Expected columns: ' + ', '.join(expected_columns),
                    'Actual columns: ' + ', '.join(self.actual_columns),
                    'Additional columns: ' +
                    ', '.join(set(self.actual_columns).difference(
                        expected_columns)),
                ]))

            self.interchange_file["header"] = self.actual_columns
            return True
        else:
            self.logger.warn(
                '\n'.join([
                    'Expected columns: ' + ', '.join(expected_columns),
                    'Actual columns: ' + ', '.join(self.actual_columns),
                    'Columns that are missing: ' +
                    ', '.join(set(expected_columns).difference(
                        self.actual_columns)),
                ]))
            return False

    #Change from S3 to a local location
    def get_output_path(self):
        return 's3://{bucket}/{output_path}/'.format(
            bucket=self.conf['OutputBucket'],
            output_path=self.conf['OutputPath'].replace("/TMP/", "/VALIDATION/"),
        )

    #Optional - write the failures in parquet
    def write_files(self, df, output_path):
        """
        write DataFrame if it contains more than zero rows.
        :param df: processed spark DataFrame
        :param output_path:
        """

        self.logger.warn(
            "Write parquet after Field validation failed with Spark csv reader")
        df.write.parquet(
            output_path,
            mode='overwrite',
            partitionBy=None,
            compression="snappy",
        )

    def get_read_schema(self):
        """
        Generate Dataframe Schema from metadata json.
        E.g.: schema = StructType([StructField('REPORTING_DAY', StringType(), True)])

        :return: StructType schema
        """

        fields = [
            StructField(
                col,
                StringType(),
                True,
            )
            for col in self.actual_columns
        ]
        fields.append(StructField(self.corruptcol_name,
                                  StringType(), nullable=True))

        return StructType(fields)

    def hotfix_validate_number_of_fields(self, metadata, object_list):
        """
        sc.TextFile cannot handle newline char whitin a field. 
        Therefore in such cases we use spark csv reader
        """

        delim = self.get_field_separator(metadata)
        quote_char = self.get_string_separator(metadata)
        # FIXME Escape character should be configurable from metadata instead of duplicating quote character
        escape_char = quote_char

        self.logger.info("Field separator is: %s" % delim)
        self.logger.info("Quote character is: %s" % quote_char)

        schema = self.get_read_schema()
        self.logger.info("Used schema is: %s" % str(schema))
        #Enumerator - file types: CSV, Parquet or JSON
        df = self.spark.read \
            .option('quote', quote_char) \
            .option('escape', escape_char) \
            .option("header", "true") \
            .option('delimiter', delim) \
            .option('enforceSchema', 'false') \
            .option('ignoreLeadingWhiteSpace', True) \
            .option('ignoreTrailingWhiteSpace', True) \
            .option('multiLine', True)\
            .option('columnNameOfCorruptRecord', self.corruptcol_name)\
            .csv(object_list,
                 schema=schema, mode='PERMISSIVE')

        # Check if CSV includes a single unnamed trailing field, ignoring count mismatch if yes
        actual_schema = df.schema.names
        last_but_one_index = len(df.schema.names) - 2
        expected_incorrect_field = ""
        if actual_schema[last_but_one_index] == expected_incorrect_field:
            self.logger.warn(f"CSV in [{object_list}] contains an additional empty trailing column without a name, "
                             f"ignoring count mismatch.")
            return True

        badRows = df.filter(F.col(self.corruptcol_name).isNotNull())
        badRows.cache()

        if bool(badRows.head(1)):
            self.logger.error(
                "Field validation failed also with Spark csv reader")
            self.write_files(badRows, self.get_output_path())
            return False

        badRows.unpersist()
        self.logger.warn(
            "Field validation was successful with Spark csv reader")
        self.interchange_file["is_multiline"] = True
        return True

    def validate_number_of_fields(self, rdd, metadata, object_list):
        """
        Every rows should contain the same number of field.
        Method:
        iterate thought the rdd, count the delimiter, and compare it with the desired number.
        zip the rows with index to easily identify the first bad row. Format: [(column_count , index)]. e.g.:[(4, 1)]
        Filter rows, which has less fields, than num_of_cols 
        :param rdd: input file
        :param metadata: data descriptor from data provider
        :param object_list: the list of input objects
        :return: True, if every row contains same number of field, False otherwise.
        """

        self.logger.info("validate_number_of_fields")
        num_of_cols = len(self.actual_columns)
        delimiter = metadata['FieldSeparator']

        len_rdd = rdd.map(lambda x: len(x.split(delimiter)))

        fields_in_rdd = len_rdd.zipWithIndex().filter(
            lambda x: x[0] != num_of_cols)
        number_boolean = True if fields_in_rdd.count() == 0 else False

        self.interchange_file["is_multiline"] = False

        if not number_boolean:
            self.logger.error('number of fields mismatch: ')
            self.logger.error('expected number of fields %d' % num_of_cols)
            self.logger.error(
                'number of fields in rdd in format [( expected count - 1, index)]: %s' % fields_in_rdd.take(1))

            self.logger.error("Try with Spark csv reader")
            number_boolean = self.hotfix_validate_number_of_fields(
                metadata, object_list)

        return number_boolean

    def validate_number_of_fields_wFieldSeparator(self, rdd, metadata, object_list):
        """
        Same as number_of fields, just for the enclosed fields.
        regex from:
        https://stackoverflow.com/questions/2785755/how-to-split-but-ignore-separators-in-quoted-strings-in-python

        :param rdd: input file
        :param metadata: data descriptor from data provider
        :return: True, if every row contains same number of field, False otherwise.
        """
        self.logger.info('validate number of fields with FieldSeparator')
        num_of_cols = len(self.actual_columns)
        regex_str = '''(?:(?:[^{sep}{quote}]|{quote}[^{quote}]*(?:{quote}|$))+|(?={sep}{sep})|(?={sep}$)|(?=^{sep}))'''
        # We set double quote character as default one if metadata does not provide it
        # (so we don't crash our above regex)
        quote_char = metadata.get('StringSeparator') or '"'
        self.logger.info('Quote character is > {} <'.format(quote_char))
        separator_char = metadata['FieldSeparator']
        self.logger.info(
            'Separator character is > {} <'.format(separator_char))
        # TODO Expand list of characters needing escaping for regex
        characters_to_escape = ('|',)
        # TODO Consider escaping also quote character
        # (a function that escapes character for use with regex would be awesome)
        if separator_char in characters_to_escape:
            separator_char = '\\' + separator_char
        prepared_regex_str = regex_str.format(
            sep=separator_char,
            quote=quote_char,
        )
        field = re.compile(prepared_regex_str)
        len_rdd = rdd.map(lambda x: len(field.findall(x)))

        fields_in_rdd = len_rdd.zipWithIndex().filter(
            lambda x: x[0] != num_of_cols)
        number_boolean = True if fields_in_rdd.count() == 0 else False

        self.interchange_file["is_multiline"] = False

        if not number_boolean:
            self.logger.error('number of fields mismatch: ')
            self.logger.error('expected number of fields %d' % num_of_cols)
            self.logger.error(
                'number of fields in rdd in format [(expected count - 1, index)]: %s' % fields_in_rdd.take(1))

            self.logger.error("Try with Spark csv reader")
            number_boolean = self.hotfix_validate_number_of_fields(
                metadata, object_list)

        return number_boolean

    def perform_validation(self, table, metadata):
        validation_result = []
        self.setup_sc()
        self.setup_log()
        self.logger.info('validation started')
        #rdd = self.sc.textFile(','.join(object_list))
        rdd = table.rdd

        # perform validation steps:
        validation_result.append(self.validate_column_names(rdd, metadata))

        if metadata.get("StringSeparator"):
            validation_result.append(
                self.validate_number_of_fields_wFieldSeparator(rdd, metadata, object_list))
        else:
            validation_result.append(
                self.validate_number_of_fields(rdd, metadata, object_list))

        #self.put_files_to_interchange(self.get_s3_object("validation_"), self.interchange_file)
        return validation_result

    def validation_main(self, table, metadata):
        if not table:
            raise ValueError('The table is not provided')

        if not metadata:
            raise ValueError('Metadata not provided or could not be loaded')

        validation_result = self.perform_validation(table, metadata)

        # TODO add report and exception
        if all(validation_result):
            print('validation success')
        else:
            print("validation failed!")
            raise ValidationError('At least one of the validations failed')


if __name__ == '__main__':
    IngestionValidation().validation_main()
