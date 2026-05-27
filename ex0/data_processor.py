from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.counter = 0
        self.strings = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass
    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return (0,"")


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)
        else:
            return isinstance(data, (int, float))


    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")
        if isinstance(data, list):
            for element in data:
                self.strings.append(str(element))
        else:
            self.strings.append(str(data))




class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return super().validate(data)


    def ingest(self, data: Any) -> None:
        return super().ingest(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return super().validate(data)


    def ingest(self, data: Any) -> None:
        return super().ingest(data)


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")



if __name__ == "__main__":
    main()
