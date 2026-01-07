import os
import sys

# Allow imports from ../src (sibling directory)
# This is needed because Python only searches the script's own directory by default
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datasets import load_dataset
from prompt_utils import extract_SOAP_response

if __name__ == "__main__":
    dataset = load_dataset("omi-health/medical-dialogue-to-soap-summary")
    for split in dataset.keys():
        dataset[split] = dataset[split].map(
            lambda x: extract_SOAP_response(x["soap"]),
            remove_columns=["soap"],
        )
    dataset.save_to_disk("soap_dataset")