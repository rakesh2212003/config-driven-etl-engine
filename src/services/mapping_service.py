from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.core.logger import get_logger
from src.transforms.transformations import apply_transformations


logger = get_logger(__name__)


def apply_mapping(df:DataFrame, mapping_config:dict) -> DataFrame:
    try:
        logger.info(
            "Applying mapping configuration"
        )

        columns_config = sorted(
            mapping_config["columns"],
            key=lambda item: item.get("seq", 0)
        )

        selected_columns = []

        for column_config in columns_config:
            source_field = column_config["source_field"]
            target_field = column_config["target_field"]
            data_type = column_config.get("data_type")
            transformations = column_config.get(
                "transformations",
                []
            )

            df = df.withColumnRenamed(
                source_field,
                target_field
            )

            if transformations:
                df = apply_transformations(
                    df=df,
                    column_name=target_field,
                    transformations=transformations
                )

            if data_type:
                df = df.withColumn(
                    target_field,
                    col(target_field).cast(data_type)
                )

            selected_columns.append(target_field)

        df = df.select(*selected_columns)

        logger.info(
            f"Mapping applied successfully. "
            f"Columns: {selected_columns}"
        )

        return df

    except Exception as error:
        logger.exception(
            "Failed to apply mapping configuration"
        )
        raise error
    