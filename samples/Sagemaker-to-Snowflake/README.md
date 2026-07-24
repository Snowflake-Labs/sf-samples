# SageMaker to Snowflake ML Migration Playbook Examples

This repository is part of a broader **migration playbook** designed to help customers move their **machine learning workloads closer to where their data already resides — in Snowflake**.  
By eliminating unnecessary data movement and leveraging Snowflake ML’s native capabilities, customers can accelerate model development, simplify deployment, and improve governance.

### Included Examples

* **XGBoost Classifier**
  - Training and inference in SageMaker vs. Snowflake ML.  
  - Demonstrates how Snowflake ML integrates directly with Snowpark DataFrames.
    
* **PyTorch**
  - Compares SageMaker’s distributed training to Snowflake’s built-in support.  
  
* **Image Classification**
  - Shows how data can be staged, transformed, and consumed natively in Snowflake.  

##  Why Snowflake ML?

- **Data stays in Snowflake**: No need to move data out to train, evaluate, or serve models.  
- **Seamless integration with Pandas/Snowpark**: Work with Snowflake data as familiar **Pandas DataFrames** or **Snowpark DataFrames**.  
- **Unified platform**: Model development, registry, and deployment happen within the same governed environment as your data.  
- **Cost & latency benefits**: Avoid data egress and reduce pipeline complexity.  



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

