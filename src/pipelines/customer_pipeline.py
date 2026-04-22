from src.pipelines.base_pipeline import BasePipeline
from src.io.reader import read_data
from src.io.writer import write_data
from src.services.mapping_service import MappingService
from src.core.config import ConfigLoader


class CustomerPipeline(BasePipeline):

    def __init__(self, spark):
        super().__init__()
        self.spark = spark

        self.input_path = "data/raw/customer.csv"
        self.output_path = "data/processed/customer"
        self.mapping_path = "config/mappings/customer.json"

    def extract(self):
        return read_data(
            self.spark,
            self.input_path,
            fmt="csv",
            options={"header": True}
        )

    def transform(self, df):
        mapping_config = ConfigLoader.load_json(self.mapping_path)
        return MappingService.apply(df, mapping_config)

    def load(self, df):
        write_data(df, self.output_path)
        return True