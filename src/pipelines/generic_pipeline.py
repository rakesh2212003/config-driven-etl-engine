from pyspark.sql.functions import col, max as spark_max

from src.core.config_loader import (
    load_yaml,
    load_json
)
from src.core.logger import get_logger
from src.core.session import get_spark_session

from src.io.reader import read_dataframe
from src.io.writter import write_dataframe

from src.services.mapping_service import (
    apply_mapping
)
from src.services.bookmark_service import (
    load_bookmark,
    update_bookmark
)
from src.services.scd_service import (
    apply_scd2
)


logger = get_logger(__name__)


class GenericPipeline:

    def __init__(self) -> None:

        self.spark = get_spark_session()

        self.app_config = load_yaml(
            "config/app.yaml"
        )

    def run(self, table_name: str) -> None:

        try:
            logger.info(
                f"Starting pipeline for table: "
                f"{table_name}"
            )

            # paths
            raw_dir = self.app_config["paths"]["raw_dir"]

            processed_dir = (
                self.app_config["paths"]
                ["processed_dir"]
            )

            input_format = (
                self.app_config["formats"]
                ["input"]
            )

            output_format = (
                self.app_config["formats"]
                ["output"]
            )

            input_path = (
                f"{raw_dir}/{table_name}.csv"
            )

            mapping_path = (
                "config/mappings/data/"
                f"{table_name}.json"
            )

            bookmark_path = (
                "config/mappings/bookmark/"
                f"{table_name}.json"
            )

            # configs
            mapping_config = load_json(
                mapping_path
            )

            bookmark_config = load_bookmark(
                bookmark_path
            )

            # read
            dataframe = read_dataframe(
                spark=self.spark,
                input_path=input_path,
                file_format=input_format,
                options={
                    "header": True,
                    "inferSchema": True
                }
            )

            rows_read = dataframe.count()

            logger.info(
                f"Rows read: {rows_read}"
            )

            # incremental filter
            bookmark_column = (
                bookmark_config
                ["bookmark_column"]
            )

            last_processed_value = (
                bookmark_config
                ["last_processed_value"]
            )

            dataframe = dataframe.filter(
                col(bookmark_column)
                > last_processed_value
            )

            logger.info(
                f"Applied bookmark filter: "
                f"{bookmark_column} > "
                f"{last_processed_value}"
            )

            # mapping
            dataframe = apply_mapping(
                dataframe=dataframe,
                mapping_config=mapping_config
            )

            # scd2
            dataframe = apply_scd2(
                dataframe=dataframe
            )

            output_table = (
                mapping_config["table_name"]
            )

            output_path = (
                f"{processed_dir}/"
                f"{output_table}"
            )

            # write
            write_dataframe(
                dataframe=dataframe,
                output_path=output_path,
                file_format=output_format,
                write_mode=(
                    mapping_config["write_mode"]
                ),
                options=(
                    mapping_config["hudi"]
                    ["options"]
                )
            )

            # bookmark update
            max_bookmark_value = (
                dataframe
                .agg(
                    spark_max(
                        col("effective_from")
                    )
                )
                .collect()[0][0]
            )

            if max_bookmark_value:

                update_bookmark(
                    bookmark_path=bookmark_path,
                    bookmark_column=(
                        bookmark_column
                    ),
                    bookmark_value=str(
                        max_bookmark_value
                    )
                )

            logger.info(
                f"Pipeline completed successfully "
                f"for table: {table_name}"
            )

        except Exception as error:
            logger.exception(
                f"Pipeline failed for table: "
                f"{table_name}"
            )
            raise error
        