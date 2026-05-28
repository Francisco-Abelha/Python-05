from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.counter = 0
        self.strings: list[str] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.strings:
            return (-1, "")
        oldest_string: str = self.strings.pop(0)
        rank = self.counter
        self.counter += 1
        return (rank, oldest_string)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, list):
            return all(
                isinstance(x, (int, float))
                and not isinstance(x, bool) for x in data
            )
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
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        else:
            return isinstance(data, str)

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise Exception("Improper text data")
        if isinstance(data, list):
            for element in data:
                self.strings.append(element)
        else:
            self.strings.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(
                isinstance(x, dict) and
                all(
                    isinstance(k, str) and isinstance(y, str)
                    for k, y in x.items()
                )
                for x in data
            )
        else:
            return (
                isinstance(data, dict)
                and all(
                    isinstance(k, str)
                    and isinstance(y, str) for k, y in data.items()
                )
            )

    def ingest(self, data: dict | list[dict]) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")
        if isinstance(data, list):
            for element in data:
                self.strings.append(": ".join(element.values()))
        else:
            self.strings.append(": ".join(data.values()))


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    num_processor = NumericProcessor()
    print(f"Trying to validate input '42': {num_processor.validate(42)}")
    print(
        f"Trying to validate input 'Hello': {num_processor.validate('Hello')}"
    )
    print(
        f"Trying to validate input 'True': {num_processor.validate(True)}"
    )
    print("Test invalid ingestion of string'foo' without prior validation:")
    try:
        num_processor.ingest('foo')
    except Exception as e:
        print(f"Got exception: {e}")
    num_list: list[int | float] = [
        1,
        2,
        3,
        4,
        5
    ]
    print(f"Processing data: {num_list}")
    num_processor.validate(num_list)
    num_processor.ingest(num_list)
    print("Extracting 3 values...")
    i: int = 3
    while i > 0:
        tup = num_processor.output()
        print(f"Numeric value {tup[0]}: {tup[1]}")
        i -= 1
    print("\nTesting Text Processor")
    text_processor = TextProcessor()
    print(f"Trying to validate input '42': {text_processor.validate(42)}")
    text_list = [
        'Hello',
        'Nexus',
        'World'
    ]
    print(f"Processing data: {text_list}")
    text_processor.validate(text_list)
    text_processor.ingest(text_list)
    print("Extracting 1 value...")
    tup2 = text_processor.output()
    print(f"Text value {tup2[0]}: {tup2[1]}\n")
    print("Testing Log Processor")
    log_processor = LogProcessor()
    print(
        "Trying to validate input "
        f"'Hello': {log_processor.validate('Hello')}"
    )
    log_list = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f"Processing data: {log_list}")
    log_processor.validate(log_list)
    log_processor.ingest(log_list)
    print("Extracting 2 values")
    j: int = 2
    while j > 0:
        tup3 = log_processor.output()
        print(f"Log entry {tup3[0]}: {tup3[1]}")
        j -= 1


if __name__ == "__main__":
    main()
