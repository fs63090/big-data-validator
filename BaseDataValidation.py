import sys
import json

class BaseDataValidation:

    def get_config(self):
        try:
            return json.loads(sys.argv[1])
        except IndexError:
            print("No configuration was passed!")
            raise IndexError

    def read_input_table(self,path_to_table):
        pass

    def read_metadata_json(self,path_to_metadata):
        pass

    @abstractmethod
    def validation_main(self):
        pass

    def main(self):
        self.arguments = self.get_config()
        input_table = self.read_input_table(self.arguments['input']['path'])
        metadata = self.read_metadata_json(self.arguments['metadataPath'])
        self.validation_main(input_table,metadata)