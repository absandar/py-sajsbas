from abc import ABC, abstractmethod

class AlmacenamientoBase(ABC):
    @abstractmethod
    def guardar(self, datos: dict):
        pass
