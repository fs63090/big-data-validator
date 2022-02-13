import json
import re
import sys
from xml.dom import ValidationErr

from pyspark.sql.types import (StringType, StructField, StructType)
import pyspark.sql.functions as F
from base_data_validation import BaseDataValidation

class BigDataValidator(BaseDataValidation):

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
        return "inputs/VALIDATION/"+self.argument+"_TMP/"

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

    def read_input_table(self, metadata, table_path):
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
            .csv(table_path,
                schema=schema, mode='PERMISSIVE')
        
        return df

    def hotfix_validate_number_of_fields(self, df):
        # Check if CSV includes a single unnamed trailing field, ignoring count mismatch if yes
        actual_schema = df.schema.names
        last_but_one_index = len(df.schema.names) - 2
        expected_incorrect_field = ""
        if actual_schema[last_but_one_index] == expected_incorrect_field:
            self.logger.warn(f"CSV contains an additional empty trailing column without a name, "
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
        return True

    def validate_number_of_fields(self, rdd, metadata, table):
        """
        Every rows should contain the same number of field.
        Method:
        iterate thought the rdd, count the delimiter, and compare it with the desired number.
        zip the rows with index to easily identify the first bad row. Format: [(column_count , index)]. e.g.:[(4, 1)]
        Filter rows, which has less fields, than num_of_cols 
        :param rdd: input file
        :param metadata: data descriptor from data provider
        :param table: the table read using Spark csv
        :return: True, if every row contains same number of field, False otherwise.
        """

        self.logger.info("validate_number_of_fields")
        num_of_cols = len(self.actual_columns)
        delimiter = metadata['FieldSeparator']

        len_rdd = rdd.map(lambda x: len(x.split(delimiter)))

        fields_in_rdd = len_rdd.zipWithIndex().filter(
            lambda x: x[0] != num_of_cols)
        number_boolean = True if fields_in_rdd.count() == 0 else False


        if not number_boolean:
            self.logger.error('number of fields mismatch: ')
            self.logger.error('expected number of fields %d' % num_of_cols)
            self.logger.error(
                'number of fields in rdd in format [( expected count - 1, index)]: %s' % fields_in_rdd.take(1))

            self.logger.error("Try with Spark csv reader")
            number_boolean = self.hotfix_validate_number_of_fields(table)

        return number_boolean

    def validate_number_of_fields_wFieldSeparator(self, rdd, metadata, table):
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


        if not number_boolean:
            self.logger.error('number of fields mismatch: ')
            self.logger.error('expected number of fields %d' % num_of_cols)
            self.logger.error(
                'number of fields in rdd in format [(expected count - 1, index)]: %s' % fields_in_rdd.take(1))

            self.logger.error("Try with Spark csv reader")
            number_boolean = self.hotfix_validate_number_of_fields(table)

        return number_boolean

    def perform_validation(self, metadata):
        validation_result = []
        self.logger.info('validation started')
        path_to_table = "inputs/" + self.argument + ".csv"
        rdd = self.sc.textFile(path_to_table)
        print(rdd)
        #rdd = table.rdd

        path_to_table = "inputs/" + self.argument + ".csv"

        # perform validation steps:
        validation_result.append(self.validate_column_names(rdd, metadata))

        table = self.read_input_table(metadata,path_to_table)

        if metadata.get("StringSeparator"):
            validation_result.append(
                self.validate_number_of_fields_wFieldSeparator(rdd, metadata, table))
        else:
            validation_result.append(
                self.validate_number_of_fields(rdd, metadata, table))

        #self.put_files_to_interchange(self.get_s3_object("validation_"), self.interchange_file)
        return validation_result

    def validation_main(self,  metadata):
        if not self.argument:
            raise ValueError('The table is not provided')

        if not metadata:
            raise ValueError('Metadata not provided or could not be loaded')

        validation_result = self.perform_validation(metadata)

        # TODO add report and exception
        if all(validation_result):
            print('validation success')
        else:
            print("validation failed!")
            raise ValidationErr('At least one of the validations failed')


if __name__ == "__main__":
    BigDataValidator().main()
