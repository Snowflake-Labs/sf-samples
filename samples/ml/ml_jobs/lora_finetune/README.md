# Medical Dialogue to SOAP Note Training

This project finetunes language models to generate structured SOAP (Subjective, Objective, Assessment, Plan) notes from medical dialogues.

## Installation

```bash
pip install -r requirements.txt
```

Requires `vllm` and `arctic_training` packages.

## Dataset

Download and preprocess the dataset:

```bash
python preprocess_data.py
```

This uses the [omi-health/medical-dialogue-to-soap-summary](https://huggingface.co/datasets/omi-health/medical-dialogue-to-soap-summary) dataset from Hugging Face. The dataset contains doctor-patient transcripts paired with clinical SOAP note summaries. The goal is to train models to accurately extract and structure clinical information from conversational medical dialogues into the standardized SOAP format.

The preprocessing script will output a local `./soap_dataset/` that includes train and test splits for training and evaluating, respectively.

## Evaluation

Model outputs are evaluated using an LLM-as-judge approach with Qwen3-8B. For each medical dialogue in the test split, we instruct the LLM to generate a SOAP note. The judge model compares the predicted S, O, A, and P sections against the ground truth and assigns a pass/fail verdict for each component. The final score is the percentage of correct predictions per section across the test set.

## Baseline Performance

Baseline model: Qwen3-1.7B

| Section | Accuracy |
|---------|----------|
| S       | 35.6%    |
| O       | 52.4%    |
| A       | 52.8%    |
| P       | 54.8%    |

These scores can be improved through finetuning on the medical dialogue dataset.

## Training

Training can be performed in two ways:

**Full Finetune:**
```bash
arctic_training Qwen3-1.7B-config.yaml
```

**LoRA Adapter:**
```bash
arctic_training Qwen3-1.7B-LoRA-config.yaml
```

Note: LoRA training is ~50% faster and requires GPU memory than full finetuning while still achieving strong performance.

The training implementation uses a custom datasource (`SOAPDataSource` in [train.py](train.py)) that formats each training example with a system prompt and user prompt. The system prompt defines the task and SOAP formatting requirements, while the user prompt contains the medical dialogue. This structured prompt engineering improves the model's ability to generate accurate, well-formatted SOAP notes.

**Troubleshooting:** If you encounter OOM (out of memory) errors during training, reduce the `data.max_length` parameter in the configuration file.

## Evaluation

To evaluate a trained model:

```bash
python eval.py <model_name_or_path> [--lora_path <path_to_adapter>]
```

For example:
```bash
# Full finetune
python eval.py ./ckpt/Qwen3-1.7B-finetuned

# LoRA adapter
python eval.py Qwen/Qwen3-1.7B --lora_path ./ckpt/Qwen3-1.7B-LoRA
```

## Results

| Section | Qwen3-1.7B | Finetuned | LoRA Adapter |
|---------|------------|-----------|--------------|
| S       | 35.6%      | **73.6%** | **64.4%**    |
| O       | 52.4%      | **72.4%** | **56.8%**    |
| A       | 52.8%      | **69.2%** | **50.8%**    |
| P       | 54.8%      | **70.8%** | **64.8%**    |
