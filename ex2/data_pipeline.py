from typing import Any, Protocol
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.counter: int = 0
        self.strings: list[str] = []
        self.ingest_calls: int = 0
        self.name: str = ""

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
    def __init__(self) -> None:
        super().__init__()
        self.name: str = "Numeric Processor"

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
                self.ingest_calls += 1
        else:
            self.strings.append(str(data))
            self.ingest_calls += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name: str = "Text Processor"

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
                self.ingest_calls += 1
        else:
            self.strings.append(data)
            self.ingest_calls += 1


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name: str = "Log Processor"

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
                self.ingest_calls += 1
        else:
            self.strings.append(": ".join(data.values()))
            self.ingest_calls += 1


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class JSONExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            found_proc: bool = False
            for proc in self.processors:
                if proc.validate(element):
                    proc.ingest(element)
                    found_proc = True
            if not found_proc:
                print(f"DataStream error - Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
        else:
            for element in self.processors:
                print(f"{element.name}: total {element.ingest_calls} items processed, remaining {len(element.strings)} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for element in self.processors:
            plugin_list: list = []
            i: int = 0
            while i < nb:
                plugin_list.append(element.output())
                i += 1
            plugin.process_output(plugin_list)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processors\n")
    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(num_proc)
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    first_batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil isconnected'}
        ],
        42,
        ['Hi', 'five']
    ]

    print(f"Send first batch of data on stream: {first_batch}\n")
    stream.process_stream(first_batch)
    stream.print_processors_stats()

    print("Send 3 processed data from each processor to a CSV plugin:\n")
    

if __name__ == "__main__":
    main()