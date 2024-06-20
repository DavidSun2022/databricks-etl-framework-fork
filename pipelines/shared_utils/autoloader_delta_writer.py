from pipelines.base_class.delta_writer import DeltaWriter
from pipelines.base_class.data_class import WriteMode, TriggerMode

class AutoloaderDeltaWriter(DeltaWriter):

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

    def __generate_streaming_checkpoint_path(self):
        pass
