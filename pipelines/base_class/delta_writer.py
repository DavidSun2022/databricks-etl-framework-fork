from typing import Protocol, List
from pyspark.sql import DataFrame
from delta.tables import *
from abc import ABC, abstractmethod
from enum import Enum
from data_class import WriteMode, TriggerMode

class DeltaWriter(ABC):
    """
    A class to write data into the Data Lake in Delta format. It supports writing directly to the Data Lake, UC External Tables, and UC Managed Tables.

    This class supports Appends, Overwrites, SCD Type I and SCD Type II operations.

    ...

    Attributes
    ----------
    df : DataFrame
       The DataFrame to write to the Data Lake.

    write_mode : WriteMode
       This specifies where the data should be written to. Options are: DATA_LAKE_PATH, UC_EXTERNAL_TABLE, UC_MANAGED_TABLE

    trigger_mode : TriggerMode
       This specifies how the data should be written. Options are: CONTINUOUS, BATCH

    data_lake_path : str (Optional)
       Required if write_mode is DATA_LAKE_PATH. The path to write the data to in the Data Lake i.e. abfss://<container>@<storage_account>.dfs.core.windows.net/

    uc_namespace : str (Optional)
       Required if write_mode is UC_EXTERNAL_TABLE or UC_MANAGED_TABLE. The path to write the data to in the UC catalog i.e. "catalog_name.schema_name.table_name"

    Methods
    -------
    write_append(self)->None
        returns: None
        Appends the DataFrame to the Data Lake or Table. This will always be an incremental operation.

    write_overwrite(self)->None
        returns: None
        Overwrites the DataFrame to the Data Lake or Table. This will always be an complete operation.

    write_scd1(self, keys: List[str], seq_col: str, except_cols: List[str] = None)->None
        returns: None
        Merges the DataFrame to the Data Lake or Table using SCD Type 1. This will always be an incremental operation.

    write_scd2(self, keys: List[str], seq_col: str, except_cols: List[str] = None)->None
        returns: None
        Merges the DataFrame to the Data Lake or Table using SCD Type 2. This will always be an incremental operation.
    """

    @classmethod
    def __init__(
        self,
        df: DataFrame,
        write_mode: WriteMode = WriteMode.UC_EXTERNAL_TABLE,
        trigger_mode: TriggerMode = TriggerMode.BATCH,
        data_lake_path: str = None,
        uc_namespace: str = None,
    ):
        self.df = df
        self.write_mode = write_mode
        self.trigger_mode = trigger_mode
        self.data_lake_path = data_lake_path
        self.uc_namespace = uc_namespace
        
        if write_mode == WriteMode.DATA_LAKE_PATH and data_lake_path is None:
            raise ValueError("Data Lake Path is required for Write Mode: DATA_LAKE_PATH")
        if write_mode == WriteMode.UC_EXTERNAL_TABLE and data_lake_path is None and uc_namespace is None:
            raise ValueError("Data Lake Path and UC Namespace are required for Write Mode: UC_EXTERNAL_TABLE")
        if write_mode == WriteMode.UC_MANAGED_TABLE and uc_namespace is None:
            raise ValueError("UC Namespace is required for Write Mode: UC_MANAGED_TABLE")      

        # Enable CDF by Default
        spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true)

    @abstractmethod
    def __generate_streaming_checkpoint_path(self) -> str:
        """
        Generates a unique location within ADLS to store the streaming checkpoint for the asset.

        This should be deterministic based on the asset location or name.

        This uses the root path specific in the .env file.

        Returns:
            str: Cloud storage location for the checkpoint path.
        """
        pass

    @abstractmethod
    def __reset_streaming_checkpoint_path(self, path: str) -> None:
        """
        Resets the streaming checkpoint path for a given asset. This will be required for Overwrite operations.

        """
        pass

    @abstractmethod
    def __add_hash_cols(self) -> DataFrame:
        """
        Generates Suncor's hash columns for data validation.

        Returns:
            DataFrame: DataFrame with hash columns included.
        """
        pass

    @abstractmethod
    def __read_sink_delta_path(path: str) -> DeltaTable:
        """
        Reads an existing Delta Table using the Data Lake specified path. This will be required for the SCD operations.
        """
        pass

    @abstractmethod
    def write_append(self):
        """
        Appends the DataFrame to the Data Lake or Table. This will always be an incremental operation.
        """
        pass

    @abstractmethod
    def write_overwrite(self):
        """
        Overwrites the existing Table or Data Lake location with the DataFrame. This will always be an overwrite operation.

        As Autoloader returns a streaming DataFrame, rewrite operations will require resetting the checkpoint location.

        """
        pass

    @abstractmethod
    def write_scd_1(self, keys: List[str], seq_col: str, except_cols: List[str] = None):
        """
        Merges the existing DataFrame using SCD Type I with the Data Lake or Table. This will always be an overwrite operation.
        Implements the same logic as DLT Apply Changes API: https://docs.databricks.com/en/delta-live-tables/cdc.html#process-scd-type-1-updates

        As Autoloader returns a streaming DataFrame, this will need to use the foreachBatch API to write the changes.
        """
        # Can leverage: https://github.com/MrPowers/mack?tab=readme-ov-file#type-2-scd-upserts
        pass

    @abstractmethod
    def write_scd_2(self, keys: List[str], seq_col: str, except_cols: List[str] = None):
        """
        Merges the existing DataFrame using SCD Type II with the Data Lake or Table. This will always be an overwrite operation.
        Implements the same logic as DLT Apply Changes API: https://docs.databricks.com/en/delta-live-tables/cdc.html#process-scd-type-2-updates

        As Autoloader returns a streaming DataFrame, this will need to use the foreachBatch API to write the changes.
        """
        pass
