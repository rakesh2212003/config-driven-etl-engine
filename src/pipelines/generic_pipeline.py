from src.pipelines.base_pipeline import BasePipeline
from src.io.reader import read_data
from src.io.writer import write_data
from src.services.mapping_service import MappingService
from src.core.config import ConfigLoader
from src.core.logger import get_logger
from src.core.bookmark import load_bookmark, update_bookmark

from pyspark.sql.functions import col, max as spark_max


logger = get_logger(__name__)


class GenericPipeline(BasePipeline):

    def __init__(self, spark, table_name: str):
        super().__init__()
        self.spark = spark
        self.table_name = table_name

        config = ConfigLoader.load_yaml("config/app_config.yaml")

        if table_name not in config["tables"]:
            raise ValueError(f"Table '{table_name}' not found in config")

        table_config = config["tables"][table_name]

        self.input_path = table_config["input_path"]
        self.output_path = table_config["output_path"]
        self.mapping_path = table_config["mapping_path"]

        # CDC config
        self.cdc_column = table_config.get("cdc", {}).get("column")
        self.cdc_type = table_config.get("cdc", {}).get("type", "full")

    # -------------------------
    # Extract
    # -------------------------
    def extract(self):
        df = read_data(
            self.spark,
            self.input_path,
            fmt="csv",
            options={"header": True}
        )

        # 🔹 Apply CDC filter if configured
        if self.cdc_type == "incremental" and self.cdc_column:
            bookmark = load_bookmark(self.table_name)
            last_value = bookmark.get("last_processed_value", 0)

            logger.info(
                f"Applying CDC filter: {self.cdc_column} > {last_value}"
            )

            df = df.filter(col(self.cdc_column) > last_value)

        return df

    # -------------------------
    # Transform
    # -------------------------
    def transform(self, df):
        mapping_config = ConfigLoader.load_json(self.mapping_path)
        return MappingService.apply(df, mapping_config)

    # -------------------------
    # Load
    # -------------------------
    def load(self, df):
        write_data(df, self.output_path)

        # 🔹 Update bookmark
        if self.cdc_type == "incremental" and self.cdc_column:
            max_value = df.agg(spark_max(self.cdc_column)).collect()[0][0]

            if max_value is not None:
                logger.info(
                    f"Updating bookmark: {self.cdc_column} = {max_value}"
                )
                update_bookmark(
                    self.table_name,
                    "last_processed_value",
                    max_value
                )

        return True