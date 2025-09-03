# SageMaker ➜ Snowflake Migration Examples

This repository provides simple examples to help you migrate machine learning workloads from **AWS SageMaker** to **Snowflake ML**.

### Included Examples

* **XGBoost Classifier** 
* **PyTorch Classifier**
* **Image Classification** 

### Why Migrate?

* Eliminate data movement between platforms
* Use Snowflake’s built‑in governance and security
* Deploy models directly as SQL functions

### Quick Start

1. Clone the repo:

```bash
git clone https://github.com/Snowflake-Labs/sf-samples.git
cd sf-samples/samples/ml-sagemaker-to-snowflake
```

2. Install requirements:

```bash
pip install -r requirements.txt
```

3. Run an example (e.g., XGBoost):

```bash
cd xgboost_classifier
python train.py
```

### Repo Structure

```
ml-sagemaker-to-snowflake/
├─ xgboost_classifier/
├─ pytorch_classifier/
├─ image_classification/
└─ README.md
```

### License

Apache 2.0

