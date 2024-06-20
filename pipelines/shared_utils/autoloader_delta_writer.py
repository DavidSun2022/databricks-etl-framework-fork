from pipelines.base_class.delta_writer import DeltaWriter, WriteMode, TriggerMode

class AutoloaderDeltaWriter(DeltaWriter):
    # Constructor inherits from DeltaWriter

    def __generate_streaming_checkpoint_path(self):
        pass
