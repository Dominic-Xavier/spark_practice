from abc import ABC, abstractmethod

class WaterMark(ABC):

    @abstractmethod
    def _create_file_if_not_exists(self):
        pass

    @abstractmethod
    def read_watermark(self, key: str):
        pass

    @abstractmethod
    def update_watermark(self, **kwargs):
        pass
