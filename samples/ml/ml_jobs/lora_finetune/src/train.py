import argparse
import json
import pprint

from arctic_training.config.data import DataSourceConfig
from arctic_training.data.source import DataSource
from arctic_training.logging import logger
from datasets import Dataset, load_from_disk

from prompt_utils import create_user_prompt, SYSTEM_PROMPT


class SOAPDataSourceConfig(DataSourceConfig):
    path: str


class SOAPDataSource(DataSource):
    name: str = "soap"
    config: SOAPDataSourceConfig

    def load(self, config: SOAPDataSourceConfig, split: str) -> Dataset:
        ds = load_from_disk(config.path)[split]
        messages = []
        for row in ds:
            try:
                messages.append(
                    {
                        "messages": [
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": create_user_prompt(row["dialogue"])},
                            {"role": "assistant", "content": json.dumps(dict(S=row["S"], O=row["O"], A=row["A"], P=row["P"]))},
                        ]
                    }
                )
            except Exception as e:
                logger.warning(f"Skipping row due to error: {e}")
                continue

        return Dataset.from_list(messages)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load SOAP dataset")
    parser.add_argument("--path", type=str, default="./soap_dataset", help="Path to the dataset")
    parser.add_argument("--split", type=str, default="train", help="Dataset split to load")
    args = parser.parse_args()

    print(f"Loading dataset from {args.path}, split={args.split}...")
    print("To train the model run: `arctic_training Qwen3-1.7B-config.yaml`")

    config = SOAPDataSourceConfig(path=args.path)
    source = SOAPDataSource(None, config=config)
    dataset = source.load(config, split=args.split)

    print(f"Loaded {len(dataset)} examples from the dataset.")
    print("Example:")
    pprint.pprint(dataset[0], width=120)
