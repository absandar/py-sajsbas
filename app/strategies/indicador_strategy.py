from abc import ABC, abstractmethod

class IndicadorStrategy(ABC):

    @abstractmethod
    def enviar_comando(self, ser):
        pass

    @abstractmethod
    def parsear(self, raw: str):
        pass

    def sniff(self, raw: str) -> bool:
        """Opcional: detectar si el mensaje parece de este modelo"""
        return False
